const crypto = require('crypto')
const fs = require('fs')
const net = require('net')
const path = require('path')
const zlib = require('zlib')
const { spawn } = require('child_process')
const { performance } = require('perf_hooks')
const FLEXIBLE_FROM = require('../src/protocol/flexibleVersions')

const NODES = Object.freeze(['1', '2', '3'])
const FILE_CAP = 2 * 1024 * 1024
const RECORD_CAP = 4096
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

/**
 * Normalizes canonical and Kafka base64url UUIDs without changing their bits.
 * @param {string} value UUID representation
 * @returns {string} lower-case canonical UUID
 * @throws {Error} for a representation that is not exactly sixteen bytes
 */
const canonicalUuid = (value) => {
  if (typeof value !== 'string') throw new Error('Invalid client instance UUID')
  if (UUID.test(value)) return value.toLowerCase()
  if (!/^[A-Za-z0-9_-]{22}$/.test(value)) throw new Error('Invalid client instance UUID')
  const bytes = Buffer.from(value, 'base64url')
  if (bytes.length !== 16 || bytes.toString('base64url') !== value)
    throw new Error('Invalid client instance UUID')
  const hex = bytes.toString('hex')
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join('-')
}

/**
 * Reads three independent broker sinks with no watcher, timer or retained file descriptor.
 * Cursors belong to one immutable run directory.
 */
class TelemetryExportReader {
  /**
   * @param {string} directory fresh absolute run sink directory
   * @param {object} [options] sink limits; the existing lane keeps its 2 MiB file cap
   * @param {number} [options.fileBytes] maximum bytes of each complete node sink
   * @returns {TelemetryExportReader} bounded reader
   * @throws {Error} for a relative directory
   */
  constructor(directory, { fileBytes = FILE_CAP } = {}) {
    if (!path.isAbsolute(directory)) throw new Error('Expected an absolute export directory')
    if (!Number.isSafeInteger(fileBytes) || fileBytes < 1)
      throw new Error('Expected a positive sink file cap')
    this.directory = directory
    this.fileBytes = fileBytes
  }

  /**
   * @returns {Object<string,number>} current sequence per source node
   * @throws {Error} on corrupt or overflowing evidence
   */
  checkpoint() {
    const rows = this.readExports()
    return Object.fromEntries(
      NODES.map((node) => [
        node,
        Math.max(0, ...rows.filter((r) => r.node === node).map((r) => r.seq)),
      ])
    )
  }

  /**
   * Validates all records before UUID filtering; retries only an incomplete final line.
   * Truncated semantic detail is retained for full independent wire decoding.
   * @param {{uuid?:string,after?:Object<string,number>}} [options] per-node selection
   * @returns {object[]} node/sequence-ordered records with canonical UUIDs
   * @throws {Error} on overflow, corruption, replaced sink or invalid cursor
   */
  readExports({ uuid, after = {} } = {}) {
    const wanted = uuid === undefined ? undefined : canonicalUuid(uuid)
    if (
      !after ||
      typeof after !== 'object' ||
      Array.isArray(after) ||
      Object.keys(after).some((node) => !NODES.includes(node)) ||
      Object.values(after).some((n) => !Number.isSafeInteger(n) || n < 0)
    )
      throw new Error('Invalid per-node export cursor')
    const checkOverflow = () => {
      if (fs.readdirSync(this.directory).some((name) => name.startsWith('OVERFLOW-')))
        throw new Error('Telemetry fixture overflow')
    }
    checkOverflow()
    const result = []
    for (const node of NODES) {
      const file = path.join(this.directory, 'exports-' + node + '.jsonl')
      let fd
      try {
        fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW)
      } catch (error) {
        if (error.code === 'ENOENT' && !(after[node] > 0)) continue
        throw error
      }
      let rows
      try {
        const stat = fs.fstatSync(fd)
        if (!stat.isFile() || stat.size > this.fileBytes) throw new Error('Telemetry sink file cap')
        rows = this.#readRows(fd, stat.size)
      } finally {
        fs.closeSync(fd)
      }
      // Reporter callbacks assign identities before concurrently enqueueing their rows.
      rows.sort((a, b) => a.seq - b.seq)
      let previous = 0
      for (const row of rows) {
        if (
          row.v !== 1 ||
          String(row.node) !== node ||
          !Number.isSafeInteger(row.seq) ||
          row.seq !== previous + 1
        )
          throw new Error('Telemetry sink sequence or node mismatch')
        previous = row.seq
        if (
          row.stub ||
          row.parse !== 'ok' ||
          !Number.isSafeInteger(row.dataBytes) ||
          row.dataBytes < 0 ||
          !/^[0-9a-f]{64}$/.test(row.dataSha256) ||
          typeof row.terminating !== 'boolean' ||
          !Number.isSafeInteger(row.pushIntervalMs) ||
          row.pushIntervalMs <= 0
        )
          throw new Error('Invalid telemetry export record')
        const id = canonicalUuid(row.clientInstanceId)
        if (row.seq > (after[node] || 0) && (wanted === undefined || id === wanted))
          result.push({ ...row, node, clientInstanceId: id })
      }
      if ((after[node] || 0) > previous) throw new Error('Telemetry sink replaced after cursor')
    }
    checkOverflow()
    return result
  }

  /**
   * Parses complete JSONL records from the original stat-sized snapshot with an 8 KiB scratch
   * buffer. UTF-8 is decoded only after a full line is present; a bounded unfinished tail is
   * retried on the next read. Every complete row is retained for the unchanged ordering and
   * validation pass. No sink bytes, cursors or evidence are rewritten, cached or dropped.
   * @param {number} fd original no-follow file descriptor
   * @param {number} size bounded size captured before reading
   * @returns {object[]} complete source rows in their original file order
   * @throws {Error|SyntaxError} for a short read, overlong record or malformed complete JSON
   */
  #readRows(fd, size) {
    if (size === 0) return []
    const buffer = Buffer.alloc(2 * RECORD_CAP)
    const rows = []
    let kept = 0
    for (let position = 0; position < size;) {
      const length = Math.min(RECORD_CAP, size - position)
      if (fs.readSync(fd, buffer, kept, length, position) !== length)
        throw new Error('Telemetry sink changed during read')
      position += length
      const end = kept + length
      const bytes = buffer.subarray(0, end)
      let start = 0
      let newline
      while ((newline = bytes.indexOf(10, start)) !== -1) {
        if (newline - start > RECORD_CAP) throw new Error('Telemetry sink record cap')
        const line = bytes.toString('utf8', start, newline)
        if (Buffer.byteLength(line) > RECORD_CAP) throw new Error('Telemetry sink record cap')
        rows.push(JSON.parse(line))
        start = newline + 1
      }
      kept = end - start
      if (kept > RECORD_CAP) throw new Error('Telemetry sink record cap')
      buffer.copy(buffer, 0, start, end)
    }
    return rows
  }
}

/**
 * Reads a just-created topic's config once the serving broker has published its metadata.
 * Only topic-propagation refusals are retried; configuration contents are checked by the caller.
 * @param {object} admin connected, telemetry-disabled fixture admin
 * @param {object} resource the newly created TOPIC resource and requested config names
 * @param {AbortSignal} [signal] enclosing scenario scope
 * @returns {Promise<object>} the DescribeConfigs response
 * @throws {Error} for a permanent refusal, cancellation or propagation deadline
 */
const readCreatedTopicConfig = async (admin, resource, signal) => {
  const { NEVER, pollUntil } = require('../src/producer/__tests__/fixtures/bounded')
  let response
  await pollUntil(
    signal || NEVER,
    3000,
    async () => {
      try {
        response = await admin.describeConfigs({ resources: [resource] })
        return true
      } catch (error) {
        if (!['UNKNOWN_TOPIC_OR_PARTITION', 'NOT_LEADER_FOR_PARTITION'].includes(error.type))
          throw error
        return false
      }
    },
    { delayMs: 50, label: 'New topic configuration propagation' }
  )
  return response
}

/**
 * @param {object} answer admin response
 * @param {string} name exact resource name
 * @returns {object} one successful resource
 * @throws {Error} for missing, ambiguous or failed resource results
 */
const checkedResource = (answer, name) => {
  const rows = answer?.resources
  if (
    !Array.isArray(rows) ||
    rows.length !== 1 ||
    rows[0].resourceName !== name ||
    rows[0].resourceType !== 16 ||
    rows[0].errorCode !== 0
  )
    throw new Error('Client metrics resource operation failed')
  return rows[0]
}

/**
 * Waits for a created CLIENT_METRICS resource's values and listing to converge together.
 * API errors fail immediately; only successful but stale readbacks are retried.
 * @param {object} admin connected, telemetry-disabled fixture admin
 * @param {{type:number,name:string}} resource newly created resource
 * @param {Object<string,string>} entries exact expected configuration values
 * @param {AbortSignal} signal enclosing scenario scope
 * @returns {Promise<object>} successful resource readback within three seconds
 * @throws {Error} API failure, cancellation or deadline with the last observed mismatches
 */
const readCreatedTelemetryResource = async (admin, resource, entries, signal) => {
  const { pollUntil } = require('../src/producer/__tests__/fixtures/bounded')
  const names = Object.keys(entries)
  const label = 'Client metrics resource readback'
  const deadline = performance.now() + 3000
  const timeout = () => Math.max(1, Math.ceil(deadline - performance.now()))
  let read
  let actual
  let listing
  const differences = () => [
    ...names
      .filter((key) => actual?.[key] !== entries[key])
      .map(
        (key) =>
          `${key}: expected ${JSON.stringify(entries[key])}, got ${JSON.stringify(actual?.[key])}`
      ),
    ...(listing?.resources.some((r) => r.type === 16 && r.name === resource.name)
      ? []
      : ['missing from listing']),
  ]
  try {
    await pollUntil(
      signal,
      3000,
      async (scope) => {
        read = checkedResource(
          await admin.describeConfigs({
            timeout: timeout(),
            resources: [{ ...resource, configNames: names }],
          }),
          resource.name
        )
        actual = Object.fromEntries(read.configEntries.map((e) => [e.configName, e.configValue]))
        scope.throwIfAborted()
        listing = await admin.listConfigResources({ resourceTypes: [16], timeout: timeout() })
        scope.throwIfAborted()
        return differences().length === 0
      },
      { delayMs: 50, label }
    )
  } catch (error) {
    if (signal.aborted || error?.message !== `${label} did not settle within 3000 ms`) throw error
    throw new Error(
      `${error.message}; ${differences().join('; ')}; ` +
        `last config=${JSON.stringify(actual)}; last listing=${JSON.stringify(listing?.resources)}`,
      { cause: error }
    )
  }
  return read
}

/**
 * Owns one initially absent CLIENT_METRICS resource through readback and verified deletion.
 * The supplied connected admin has telemetry disabled. Deletion is attempted after an
 * ambiguous create result; a primary failure remains first if cleanup also fails. Creation
 * readback polls for up to three seconds; deletion keeps its independent one-second window.
 * @param {object} admin completed configuration-resource admin API
 * @param {{name:string,entries:Object<string,string>}} config exact resource values
 * @param {function():Promise<void>} work lazy body after successful readback
 * @param {{now?:function():number,signal?:AbortSignal}} [options] receipt clock and creation scope
 * @returns {Promise<object>} readback and verified deletion receipt, after work completes
 * @throws {Error|AggregateError} setup, work or cleanup failure
 */
const withTelemetryResource = async (
  admin,
  { name, entries },
  work,
  { now = () => performance.now(), signal } = {}
) => {
  const { NEVER } = require('../src/producer/__tests__/fixtures/bounded')
  signal ||= NEVER
  signal.throwIfAborted()
  const receipt = { name, entries: { ...entries } }
  const names = Object.keys(entries)
  const listed = (timeout = 3000) => admin.listConfigResources({ resourceTypes: [16], timeout })
  const has = (answer) => answer.resources.some((r) => r.type === 16 && r.name === name)
  if (has(await listed())) throw new Error('Client metrics resource already exists')
  signal.throwIfAborted()
  const resource = { type: 16, name }
  const alter = (operation, values) =>
    admin.incrementalAlterConfigs({
      timeout: 3000,
      resources: [
        {
          ...resource,
          configEntries: names.map((key) => ({
            name: key,
            operation,
            ...(values ? { value: values[key] } : {}),
          })),
        },
      ],
    })
  let primary
  let failed = false
  try {
    checkedResource(await alter(0, entries), name)
    receipt.readback = await readCreatedTelemetryResource(admin, resource, entries, signal)
    signal.throwIfAborted()
    receipt.createdAt = now()
    await work()
  } catch (error) {
    primary = error
    failed = true
  }
  let cleanup
  let cleanupFailed = false
  try {
    const deletion = (receipt.deletion = { startedAt: now() })
    deletion.response = await alter(1)
    deletion.responseAt = now()
    checkedResource(deletion.response, name)
    // The controller acknowledgement can precede a broker's metadata publication. Keep the
    // absence proof, but allow a bounded propagation window; API errors still fail immediately.
    const deadline = performance.now() + 1000
    for (;;) {
      deletion.listing = await listed(Math.max(1, Math.ceil(deadline - performance.now())))
      deletion.listedAt = now()
      if (!has(deletion.listing)) break
      const remaining = deadline - performance.now()
      if (remaining <= 0) throw new Error('Client metrics resource deletion not observed')
      await new Promise((resolve) => setTimeout(resolve, Math.min(50, remaining)))
    }
  } catch (error) {
    cleanup = error
    cleanupFailed = true
  }
  if (failed && cleanupFailed)
    throw new AggregateError([primary, cleanup], 'Telemetry resource work and cleanup failed')
  if (failed) throw primary
  if (cleanupFailed) throw cleanup
  return receipt
}

/**
 * Reads the current sole-guard run directory without retaining resources.
 * @param {{uuid?:string,after?:Object<string,number>}} [options] selection
 * @returns {object[]} bounded independent exporter records
 * @throws {Error} for absent configuration or corrupt evidence
 */
const readExports = ({ fileBytes, ...options } = {}) =>
  new TelemetryExportReader(process.env.TELEMETRY_EVIDENCE_DIR || '', { fileBytes }).readExports(
    options
  )

const ENDPOINTS = Object.freeze({
  configured: Object.freeze({ 19092: '1', 19095: '2', 19098: '3' }),
  base: Object.freeze({ 9092: '1', 9095: '2', 9098: '3' }),
})
const HOSTS = Object.freeze(['localhost', '127.0.0.1'])
const PEER_ADDRESS = '127.0.0.1'
const KEEP_ALIVE_DELAY = 60000
const SETUP_APIS = new Set([17, 18, 36])
const TELEMETRY_APIS = new Set([71, 72])
const PAYLOAD_CAP = 4096
const UNSUPPORTED_VERSION = 35
const DECODER_SCRIPT = path.join(__dirname, '..', 'scripts', 'e2e', 'telemetry', 'run.sh')
const DEFAULT_LIMITS = Object.freeze({
  sockets: 64,
  pending: 128,
  records: 4096,
  retained: 2 * 1024 * 1024,
  frame: 1048576,
  prefix: 8192,
})

/**
 * Counting limiter for the independent decoder children of every observer that shares it.
 */
class DecoderGate {
  #limit
  #running = 0
  #queue = []

  /**
   * @param {number} limit most tasks running at once
   * @throws {Error} for a limit that is not a positive integer
   */
  constructor(limit) {
    if (!Number.isSafeInteger(limit) || limit < 1)
      throw new Error('Expected a positive decoder concurrency')
    this.#limit = limit
  }

  /** @returns {{running:number,queued:number}} current occupancy */
  occupancy() {
    return { running: this.#running, queued: this.#queue.length }
  }

  /**
   * @template T
   * @param {function():Promise<T>} task started once a slot is free
   * @returns {Promise<T>} the task's own outcome
   */
  run(task) {
    return new Promise((resolve, reject) => {
      const start = () => {
        this.#running++
        Promise.resolve()
          .then(task)
          .then(resolve, reject)
          .finally(() => {
            this.#running--
            this.#queue.shift()?.()
          })
      }
      if (this.#running < this.#limit) start()
      else this.#queue.push(start)
    })
  }
}

/**
 * Bounds-checked big-endian reader over one captured frame.
 */
class WireCursor {
  /**
   * @param {Buffer} buffer captured bytes
   */
  constructor(buffer) {
    this.buffer = buffer
    this.offset = 0
  }

  /** @returns {number} bytes not yet consumed */
  remaining() {
    return this.buffer.length - this.offset
  }

  /**
   * @param {number} count bytes to consume
   * @returns {Buffer} a view of the consumed bytes
   * @throws {Error} when the frame is shorter than its own fields
   */
  take(count) {
    if (!Number.isSafeInteger(count) || count < 0 || count > this.remaining())
      throw new Error('Truncated frame observed on the wire')
    const view = this.buffer.subarray(this.offset, this.offset + count)
    this.offset += count
    return view
  }

  /** @returns {number} signed byte */
  int8() {
    return this.take(1).readInt8(0)
  }

  /** @returns {number} signed 16-bit integer */
  int16() {
    return this.take(2).readInt16BE(0)
  }

  /** @returns {number} signed 32-bit integer */
  int32() {
    return this.take(4).readInt32BE(0)
  }

  /**
   * @returns {number} finite unsigned varint of at most five bytes
   * @throws {Error} for an unterminated varint
   */
  varuint() {
    let value = 0
    for (let index = 0; index < 5; index++) {
      const byte = this.take(1)[0]
      value += (byte & 0x7f) * 2 ** (7 * index)
      if (!(byte & 0x80)) return value
    }
    throw new Error('Invalid unsigned varint on the wire')
  }

  /** @returns {string|null} classic nullable string */
  nullableString() {
    const length = this.int16()
    return length < 0 ? null : this.take(length).toString('utf8')
  }

  /**
   * @returns {number} element count of a non-null compact array
   * @throws {Error} for a null array or a count larger than the remaining bytes
   */
  compactCount() {
    const count = this.varuint() - 1
    if (count < 0 || count > this.remaining()) throw new Error('Invalid compact array on the wire')
    return count
  }

  /** @returns {string} compact non-null string */
  compactString() {
    const length = this.varuint() - 1
    if (length < 0) throw new Error('Null compact string on the wire')
    return this.take(length).toString('utf8')
  }

  /** @returns {boolean} strict boolean byte */
  bool() {
    const value = this.int8()
    if (value !== 0 && value !== 1) throw new Error('Invalid boolean on the wire')
    return value === 1
  }

  /** Skips a bounded tagged-field section. */
  tags() {
    const count = this.varuint()
    for (let index = 0; index < count; index++) {
      this.varuint()
      this.take(this.varuint())
    }
  }

  /** @throws {Error} when bytes remain after the last field */
  done() {
    if (this.remaining() !== 0) throw new Error('Trailing bytes in a frame observed on the wire')
  }
}

/**
 * Splits one direction of a TCP byte stream into Kafka frames without assuming chunk
 * boundaries. It keeps a bounded prefix and a SHA-256 of each frame and discards the rest.
 */
class FrameStream {
  #header = Buffer.alloc(0)
  #parts = []
  #kept = 0
  #length = 0
  #remaining = 0
  #hash = null

  /**
   * @param {{frame:number,prefix:number}} limits maximum frame size and retained prefix
   * @param {function({length:number,prefix:Buffer,sha256:string}):void} onFrame completed frame
   * @param {function(Error):void} onError receives an error thrown by `onFrame`
   */
  constructor(limits, onFrame, onError) {
    this.limits = limits
    this.onFrame = onFrame
    this.onError = onError
  }

  /**
   * @param {Buffer} chunk next bytes of the stream
   * @throws {Error} for a frame length outside the signed 0..cap range; later bytes are unusable
   */
  push(chunk) {
    let offset = 0
    while (offset < chunk.length) {
      if (this.#header.length < 4) {
        const take = Math.min(4 - this.#header.length, chunk.length - offset)
        this.#header = Buffer.concat([this.#header, chunk.subarray(offset, offset + take)])
        offset += take
        if (this.#header.length === 4) this.#start(this.#header.readInt32BE(0))
        continue
      }
      const take = Math.min(this.#remaining, chunk.length - offset)
      const piece = chunk.subarray(offset, offset + take)
      const keep = Math.min(this.limits.prefix - this.#kept, take)
      if (keep > 0) {
        this.#parts.push(Buffer.from(piece.subarray(0, keep)))
        this.#kept += keep
      }
      this.#hash.update(piece)
      this.#remaining -= take
      offset += take
      if (this.#remaining === 0) this.#finish()
    }
  }

  /**
   * @param {number} length frame body length
   * @throws {Error} when out of range
   */
  #start(length) {
    if (length < 0 || length > this.limits.frame) throw new Error('Frame length out of range')
    this.#length = length
    this.#remaining = length
    this.#parts = []
    this.#kept = 0
    this.#hash = crypto.createHash('sha256')
    if (length === 0) this.#finish()
  }

  /** Emits the completed frame and resets for the next one. */
  #finish() {
    const frame = {
      length: this.#length,
      prefix: Buffer.concat(this.#parts),
      sha256: this.#hash.digest('hex'),
    }
    this.#header = Buffer.alloc(0)
    this.#parts = []
    this.#hash = null
    try {
      this.onFrame(frame)
    } catch (error) {
      this.onError(error)
    }
  }
}

/**
 * @param {number} apiKey request API
 * @param {number} version request version
 * @returns {boolean} whether this version uses the flexible request header (v2)
 * @throws {Error} for an API absent from the retained protocol metadata
 */
const isFlexible = (apiKey, version) => {
  const first = FLEXIBLE_FROM[apiKey]
  if (first === undefined) throw new Error(`Unknown API key ${apiKey} on the wire`)
  return first !== null && version >= first
}

/**
 * @param {Buffer} bytes sixteen UUID bytes
 * @returns {string} canonical UUID
 */
const uuidOf = (bytes) => canonicalUuid(bytes.toString('base64url'))

/**
 * Independently parses a request header and, for API 71 and 72, its whole v0 body.
 * @param {Buffer} prefix retained frame prefix
 * @param {number} length full frame length
 * @returns {object} header fields, plus uuid/subscription/compression/blob for telemetry
 * @throws {Error} for a malformed or oversized telemetry request
 */
const parseRequest = (prefix, length) => {
  const cursor = new WireCursor(prefix)
  const head = {
    apiKey: cursor.int16(),
    apiVersion: cursor.int16(),
    correlation: cursor.int32(),
    clientId: cursor.nullableString(),
  }
  if (isFlexible(head.apiKey, head.apiVersion)) cursor.tags()
  if (!TELEMETRY_APIS.has(head.apiKey)) return head
  if (head.apiVersion !== 0 || prefix.length !== length)
    throw new Error('Unexpected telemetry request version or size')
  head.uuid = uuidOf(cursor.take(16))
  if (head.apiKey === 72) {
    head.subscriptionId = cursor.int32()
    head.terminating = cursor.bool()
    head.compressionType = cursor.int8()
    const size = cursor.varuint() - 1
    if (size < 0) throw new Error('Null PushTelemetry metrics')
    const bytes = cursor.take(size)
    head.blob = Buffer.alloc(size)
    bytes.copy(head.blob)
  }
  cursor.tags()
  cursor.done()
  return head
}

/**
 * Parses an ApiVersions response body; an UNSUPPORTED_VERSION answer uses the v0 layout.
 * @param {WireCursor} cursor positioned after the correlation id
 * @param {number} version requested ApiVersions version
 * @returns {{errorCode:number,ranges:Object<string,number[]>}} advertised version ranges
 */
const parseApiVersions = (cursor, version) => {
  const errorCode = cursor.int16()
  const layout = errorCode === UNSUPPORTED_VERSION ? 0 : version
  const compact = layout >= 3
  const count = compact ? cursor.compactCount() : cursor.int32()
  if (count < 0 || count > cursor.remaining()) throw new Error('Invalid ApiVersions array')
  const ranges = {}
  for (let index = 0; index < count; index++) {
    const apiKey = cursor.int16()
    ranges[apiKey] = [cursor.int16(), cursor.int16()]
    if (compact) cursor.tags()
  }
  if (layout >= 1) cursor.int32()
  if (compact) cursor.tags()
  cursor.done()
  return { errorCode, ranges }
}

/**
 * Independently parses the responses the observer interprets: ApiVersions, Get and Push.
 * @param {Buffer} prefix retained frame prefix
 * @param {number} length full frame length
 * @param {{apiKey:number,apiVersion:number}} request the pending request it answers
 * @returns {object} parsed response fields; empty for other APIs
 * @throws {Error} for a malformed or oversized parsed response
 */
const parseResponse = (prefix, length, request) => {
  if (prefix.length !== length) throw new Error('Oversized response for a parsed API')
  const cursor = new WireCursor(prefix)
  cursor.int32()
  if (request.apiKey === 18) return parseApiVersions(cursor, request.apiVersion)
  if (isFlexible(request.apiKey, request.apiVersion)) cursor.tags()
  const parsed = { throttleMs: cursor.int32(), errorCode: cursor.int16() }
  if (request.apiKey === 71) {
    parsed.uuid = uuidOf(cursor.take(16))
    parsed.subscriptionId = cursor.int32()
    parsed.acceptedCompressionTypes = Array.from({ length: cursor.compactCount() }, () =>
      cursor.int8()
    )
    parsed.pushIntervalMs = cursor.int32()
    parsed.telemetryMaxBytes = cursor.int32()
    parsed.deltaTemporality = cursor.bool()
    parsed.requestedMetrics = Array.from({ length: cursor.compactCount() }, () =>
      cursor.compactString()
    )
  }
  cursor.tags()
  cursor.done()
  return parsed
}

/**
 * Bounded decompression of a captured Push body with built-in codecs only.
 * @param {number} type observed compression type: 0 none, 1 gzip, 4 zstd
 * @param {Buffer} blob transmitted bytes
 * @returns {Buffer} decompressed MetricsData of at most 4096 bytes
 * @throws {Error} for an unsupported codec or output above the cap
 */
const decompress = (type, blob) => {
  const options = { maxOutputLength: PAYLOAD_CAP }
  if (type === 0) return blob
  if (type === 1) return zlib.gunzipSync(blob, options)
  // eslint-disable-next-line n/no-unsupported-features/node-builtins -- Node >= 22.15 ships it; absence is a readiness failure
  if (type === 4) return zlib.zstdDecompressSync(blob, options)
  throw new Error(`Unsupported observed telemetry compression type ${type}`)
}

/**
 * @param {Buffer} bytes data
 * @returns {string} lower-case SHA-256 hex
 */
const sha256 = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

/**
 * Joins public disconnect without an observer receiver in its awaited stack frame. Only the
 * client, original options, borrowed clock and scalar close receipt enter this boundary.
 * @param {object} client public client
 * @param {object|undefined} options original disconnect arguments
 * @param {{now:function():number}} clock borrowed monotonic clock
 * @param {{at:number,pending:string[],end?:number}} close scalar close receipt
 * @returns {Promise<void>} after disconnect and receipt completion
 * @throws {*} the original disconnect failure, without reading its stack
 */
const closeObservedClient = async (client, options, clock, close) => {
  try {
    await client.disconnect(options)
  } finally {
    close.end = clock.now()
  }
}

/**
 * Observes the real client sockets of one public client, independently of the library. It wraps
 * `write`, listens to `data`, and records normal and telemetry traffic, so tests can join actual
 * accepted Push requests to the broker sinks and to an independent Java decode. It never changes
 * what the client writes or reads, never destroys a client socket and holds no timer or child
 * after `dispose`. All faults latch and surface through `assertHealthy`.
 */
class TelemetryWireObserver {
  #clock
  #endpoints
  #dial
  #limits
  #script
  #env
  #line
  #decoder
  #reader
  #owner
  #sockets = new Map()
  #records = []
  #blobs = new Map()
  #consumed = new Set()
  #decoded = new Map()
  #failures = []
  #children = new Set()
  #timers = new Map()
  #retained = 0
  #disposal = null
  #stillOpen = []
  #closures = []
  #faults = []
  #cancellations = []
  #completeStream
  #gate
  #captureTail = Promise.resolve()

  /**
   * @param {object} options observer configuration
   * @param {string} options.directory absolute run evidence directory shared with the broker sinks
   * @param {string} options.line broker line: 4.4.0, 4.3.1 or 4.2.2 (any x.y.z in `base` mode)
   * @param {{now:function():number}} options.clock monotonic milliseconds, such as performance.now
   * @param {'configured'|'base'} [options.mode] selects the exact fixture endpoint allowlist
   * @param {string} [options.owner] label of the observed client, kept in the evidence
   * @param {function(object, function):object} [options.dial] socket dialer, `net.connect` by default
   * @param {boolean} [options.completeStream=false] decode every accepted nonempty Push before disposal
   * @param {Partial<typeof DEFAULT_LIMITS>} [options.limits] lower bounds for controls
   * @param {number} [options.sinkBytes] sink file cap; omitted for the existing lane
   * @param {function(Buffer, number):Promise<object>} [options.decoder] decoder seam for controls
   * @param {string} [options.decoderScript] runner script, `scripts/e2e/telemetry/run.sh` by default
   * @param {object} [options.env] environment of the decoder child
   * @param {DecoderGate} [options.gate] shared limiter on concurrently running decoder children
   * @param {Set<string>} [options.consumed] sink identities (`node:seq`) already joined to a Push;
   *   share one set across observers so a sink row is consumed once for the whole run
   * @throws {Error} for a relative directory, unsupported line/mode or invalid clock
   */
  constructor({
    directory,
    line,
    clock,
    mode = 'configured',
    owner = 'client',
    dial = net.connect,
    limits = {},
    sinkBytes,
    decoder,
    completeStream = false,
    decoderScript = DECODER_SCRIPT,
    env = process.env,
    gate,
    consumed,
  }) {
    if (typeof directory !== 'string' || !path.isAbsolute(directory))
      throw new Error('Expected an absolute observer directory')
    if (!Object.hasOwn(ENDPOINTS, mode)) throw new Error('Unknown observer mode')
    const allowed =
      mode === 'configured'
        ? ['4.4.0', '4.3.1', '4.2.2'].includes(line)
        : /^\d+\.\d+\.\d+$/.test(line)
    if (!allowed) throw new Error('Unsupported broker line for the observer')
    if (typeof clock?.now !== 'function') throw new Error('Expected a monotonic clock')
    this.#completeStream = completeStream
    this.#gate = gate
    if (consumed) this.#consumed = consumed
    this.#clock = clock
    this.#endpoints = ENDPOINTS[mode]
    this.#dial = dial
    this.#limits = { ...DEFAULT_LIMITS, ...limits }
    this.#script = decoderScript
    this.#env = env
    this.#line = line
    this.#decoder = decoder
    this.#owner = owner
    this.#reader = new TelemetryExportReader(directory, { fileBytes: sinkBytes })
    this.directory = fs.mkdtempSync(path.join(directory, 'observer-'))
    this.tmp = path.join(this.directory, 'tmp')
    fs.mkdirSync(this.tmp)
  }

  /**
   * Dials a fixture endpoint and starts observing its socket. It matches the library's default
   * factory: `onConnect` is the dialer callback, keep-alive is set, and a real socket returns.
   * @param {{host:string,port:number,ssl?:object,onConnect?:function}} options factory options
   * @returns {import('net').Socket} the dialed socket
   * @throws {Error} before dialing for TLS, a foreign host or a port outside the fixture
   */
  socketFactory({ host, port, ssl, onConnect } = {}) {
    if (ssl) throw new Error('The telemetry fixture observer is plaintext only')
    const key = Number.isInteger(port) ? String(port) : ''
    if (!HOSTS.includes(host) || !Object.hasOwn(this.#endpoints, key))
      throw new Error(`The telemetry fixture refuses endpoint ${host}:${port}`)
    const socket = this.#dial({ host, port, family: 4 }, onConnect)
    socket.setKeepAlive(true, KEEP_ALIVE_DELAY)
    this.#attach(socket, port, this.#endpoints[key])
    return socket
  }

  /**
   * Captures the synchronous public-close boundary and its exact in-flight exchange. New normal
   * requests after this boundary receive no allowance; only a final issued by this close may join.
   * @param {object} client public client
   * @param {object} [options] public disconnect arguments
   * @returns {Promise<void>} after the original disconnect settles
   * @throws {*} the original disconnect failure
   */
  closeClient(client, options) {
    try {
      const close = {
        at: this.#clock.now(),
        pending: this.#records
          .filter(
            (r) =>
              r.type === 'request' &&
              TELEMETRY_APIS.has(r.apiKey) &&
              !r.response &&
              !this.#sockets.get(r.socketId).closed
          )
          .map((r) => r.requestKey),
      }
      this.#closures.push(close)
      return closeObservedClient(client, options, this.#clock, close)
    } catch (error) {
      return Promise.reject(error)
    }
  }

  /**
   * Declares a deliberately faulted captured transport before arming its one selected exchange.
   * @param {number} socketId observed transport identity
   * @returns {void}
   * @throws {Error} if no telemetry transport owns this identity
   */
  declareFault(socketId) {
    if (this.#sockets.get(socketId)?.kind !== 'telemetry')
      throw new Error('Unknown faulted transport')
    this.#faults.push({ socketId, at: this.#clock.now() })
  }

  /** @returns {string[]} exact unanswered request identities qualified by close or fault evidence */
  cancellationKeys() {
    return this.#cancellations.map((r) => r.requestKey)
  }

  /** @returns {number} issued client sockets that have not closed yet */
  openSockets() {
    return [...this.#sockets.values()].filter((state) => !state.closed).length
  }

  /**
   * After disposal, an unclosed socket remains an unresolved cleanup failure.
   * @returns {Promise<void>} once every socket issued so far has closed
   */
  async socketsClosed() {
    await Promise.all(
      [...this.#sockets.values()].map(
        (state) => state.closedPromise ?? (state.closed ? undefined : new Promise(() => {}))
      )
    )
  }

  /**
   * @returns {number} index of the last transcript record, usable as an `after` cursor
   */
  checkpoint() {
    return this.#records.length
  }

  /**
   * @param {{after?:number}} [options] return only records after this observer-local index
   * @returns {object[]} copies of connect and request/response metadata, never raw payloads
   */
  readTranscript({ after = 0 } = {}) {
    return this.#records.filter((record) => record.index > after).map(copy)
  }

  /**
   * @param {{uuid?:string,after?:number}} [options] stream and transcript cursor
   * @returns {object[]} nonterminating Push v0 requests whose own response was error 0
   */
  accepted({ uuid, after = 0 } = {}) {
    return this.#pushes(uuid, after, false)
  }

  /**
   * @param {{uuid?:string,after?:number}} [options] stream and transcript cursor
   * @returns {object[]} terminating Push v0 requests whose own response was error 0
   */
  finals({ uuid, after = 0 } = {}) {
    return this.#pushes(uuid, after, true)
  }

  /**
   * @param {{uuid?:string,after?:number}} [options] stream and transcript cursor
   * The assigned UUID in a successful response also matches, so the initial zero-ID request
   * remains visible; its original request UUID is retained in `uuid`.
   * @returns {object[]} Get requests with their parsed responses, answered or not
   */
  subscriptions({ uuid, after = 0 } = {}) {
    const wanted = uuid === undefined ? undefined : canonicalUuid(uuid)
    return this.#records
      .filter(
        (r) =>
          r.type === 'request' &&
          r.apiKey === 71 &&
          r.index > after &&
          (wanted === undefined ||
            r.uuid === wanted ||
            (r.response?.errorCode === 0 && r.response.uuid === wanted))
      )
      .map(copy)
  }

  /**
   * @returns {object[]} each socket with the 71/72 ranges its ApiVersions response advertised
   */
  advertisements() {
    return [...this.#sockets.values()]
      .filter((state) => state.ranges)
      .map((state) => ({
        socketId: state.id,
        node: state.node,
        telemetry: state.kind === 'telemetry',
        ranges: { 71: state.ranges[71] ?? null, 72: state.ranges[72] ?? null },
      }))
  }

  /** @returns {object[]} all physical sockets; foregroundBootstrap marks proven foreground negotiation */
  socketOwnership() {
    return [...this.#sockets.values()].map(({ id, kind, node, foregroundBootstrap }) => ({
      id,
      kind,
      node,
      foregroundBootstrap: !!foregroundBootstrap,
    }))
  }

  /** @returns {object[]} sockets that carried API 71 or 72 */
  telemetrySockets() {
    return this.#socketsOf('telemetry')
  }

  /** @returns {object[]} sockets with no request that classifies them as normal or telemetry */
  undecided() {
    return this.#socketsOf('undecided')
  }

  /**
   * Confirms the first foreground socket after public connect succeeds. Core connect precedes
   * telemetry resume on every factory. Other setup-only sockets require foreground fetch proof
   * or actual ordinary/telemetry traffic.
   * @param {number} [socketId=1] first socket of this observed client
   * @returns {void}
   * @throws {Error} if the socket is not the first successful foreground bootstrap
   */
  confirmForegroundBootstrap(socketId = 1) {
    const state = this.#sockets.get(socketId)
    const connects = this.#records.filter((r) => r.type === 'connect')
    if (
      socketId !== 1 ||
      connects[0]?.socketId !== socketId ||
      !state?.ranges ||
      state.kind === 'telemetry'
    )
      throw new Error('Not a proven foreground bootstrap socket')
    state.kind = 'normal'
    state.foregroundBootstrap = true
  }

  /**
   * Proves a broker's setup-only foreground socket from its paired Fetch/ShareFetch socket.
   * The pool negotiates on index zero before opening its dedicated fetch connection. Only one
   * such pair per node is permitted; telemetry advertisements, extra sockets, missing negotiation
   * or an unanswered fetch cannot establish ownership. Validate all candidates before relabelling.
   * @returns {void}
   * @throws {Error} if any undecided socket lacks exact foreground fetch setup evidence
   */
  confirmForegroundFetchSetup() {
    const candidates = [...this.#sockets.values()].filter((s) => s.kind === 'undecided')
    for (const setup of candidates) {
      const peers = [...this.#sockets.values()].filter((s) => s.node === setup.node)
      const sibling = peers.find((s) => s.id !== setup.id)
      const requests = this.#records.filter((r) => r.type === 'request' && r.socketId === setup.id)
      const negotiated = requests.findLast((r) => r.apiKey === 18 && r.response?.errorCode === 0)
      const fetch = this.#records.find(
        (r) => r.socketId === sibling?.id && [1, 78].includes(r.apiKey) && r.response
      )
      if (
        peers.length !== 2 ||
        sibling?.kind !== 'normal' ||
        !negotiated ||
        requests.some((r) => r.apiKey !== 18 || !r.response) ||
        setup.ranges[71] ||
        setup.ranges[72] ||
        !fetch ||
        !setup.ranges[fetch.apiKey] ||
        negotiated.response.responseAt >= fetch.writeAt ||
        setup.id >= sibling.id ||
        requests.some((r) => r.clientId !== fetch.clientId)
      )
        throw new Error('Not a proven foreground fetch setup socket')
    }
    for (const setup of candidates) {
      setup.kind = 'normal'
      setup.foregroundBootstrap = true
    }
  }

  /**
   * @param {{after?:number}} [options] transcript cursor
   * @returns {object[]} successful physical connects of sockets proven to carry normal traffic
   */
  normalConnects({ after = 0 } = {}) {
    return this.#records
      .filter(
        (r) =>
          r.type === 'connect' &&
          r.index > after &&
          this.#sockets.get(r.socketId)?.kind === 'normal'
      )
      .map(copy)
  }

  /**
   * @param {{after?:number}} [options] transcript cursor
   * @returns {object[]} completed request/response pairs on normal sockets
   */
  normalRequests({ after = 0 } = {}) {
    return this.#records
      .filter(
        (r) =>
          r.type === 'request' &&
          r.index > after &&
          r.response &&
          this.#sockets.get(r.socketId)?.kind === 'normal'
      )
      .map(copy)
  }

  /**
   * Joins one accepted Push to its broker sink record and independently decodes its bytes.
   * @param {object} push a record returned by `accepted` or `finals`
   * @param {{budgetMs?:number,sinkWaitMs?:number}} [options] decoder and sink-flush budgets
   * @returns {Promise<object>} requestKey, uuid, node, compression, raw/transmitted sizes, raw hash,
   *   full decoded MetricsData and the sink `exportIdentity` it consumed
   * @throws {Error} for a zero blob, an over-cap payload, no unconsumed matching sink record, or
   *   a decoder failure; each is also latched as a fixture failure
   */
  async decodeAccepted(push, { budgetMs = 3000, sinkWaitMs = 2000 } = {}) {
    try {
      const record = this.#records.find((r) => r.requestKey === push?.requestKey)
      if (!record || record.apiKey !== 72 || record.response?.errorCode !== 0)
        throw new Error('Not an accepted Push request')
      if (this.#decoded.has(record.requestKey))
        return copy(await this.#decoded.get(record.requestKey))
      const result = this.#decodeRecord(record, budgetMs, sinkWaitMs)
      this.#decoded.set(record.requestKey, result)
      return copy(await result)
    } catch (error) {
      this.#latch(error)
      throw error
    }
  }

  /**
   * Decodes and consumes a request exactly once, retaining the complete result for later uses.
   * @param {object} record accepted wire record
   * @param {number} budgetMs independent decoder budget
   * @param {number} sinkWaitMs sink flush budget
   * @returns {Promise<object>} decoded result and sidecar identity
   * @throws {Error} on a failed join or decode
   */
  async #decodeRecord(record, budgetMs, sinkWaitMs) {
    const blob = this.#blobs.get(record.requestKey)
    if (blob.length === 0) throw new Error('A zero-byte Push has no exporter record')
    if (blob.length > PAYLOAD_CAP) throw new Error('Transmitted telemetry exceeds 4096 bytes')
    const raw = decompress(record.compressionType, blob)
    if (raw.length > PAYLOAD_CAP) throw new Error('Raw telemetry exceeds 4096 bytes')
    const rawSha256 = sha256(raw)
    const rawFile = path.join(this.directory, `raw-${rawSha256}.bin`)
    if (!fs.existsSync(rawFile)) fs.writeFileSync(rawFile, raw, { flag: 'wx', mode: 0o600 })
    const row = await this.#join(record, raw.length, rawSha256, sinkWaitMs)
    const decoded = await this.decodeRaw(raw, { budgetMs })
    if (decoded.dataBytes !== raw.length || decoded.dataSha256 !== rawSha256)
      throw new Error('Independent decode does not match the captured bytes')
    const result = {
      requestKey: record.requestKey,
      uuid: record.uuid,
      node: record.node,
      compressionType: record.compressionType,
      rawBytes: raw.length,
      transmittedBytes: blob.length,
      transmittedSha256: sha256(blob),
      rawSha256,
      decoded,
      exportIdentity: { node: row.node, seq: row.seq },
    }
    const json = JSON.stringify(result)
    if (Buffer.byteLength(json) > 1048576) throw new Error('Decoded sidecar exceeds bound')
    this.#retain(Buffer.byteLength(json))
    fs.writeFileSync(path.join(this.directory, `decoded-${record.requestKey}.json`), json, {
      flag: 'wx',
      mode: 0o600,
    })
    return result
  }

  /**
   * Decodes MetricsData with the pinned reporter in a supervised process group.
   * @param {Buffer} raw decompressed MetricsData of at most 4096 bytes
   * @param {{budgetMs?:number}} [options] total budget; the group gets a further 1 s kill grace
   * @returns {Promise<object>} the decoded body, after the runner's identity line verified
   * @throws {Error} for missing cache, timeout, nonzero exit, malformed output or a surviving group
   */
  decodeRaw(raw, options) {
    return this.#gate
      ? this.#gate.run(() => this.#decodeRaw(raw, options))
      : this.#decodeRaw(raw, options)
  }

  /**
   * @param {Buffer} raw decompressed MetricsData of at most 4096 bytes
   * @param {{budgetMs?:number}} [options] total budget
   * @returns {Promise<object>} the decoded body
   * @throws {Error} for any decoder failure
   */
  async #decodeRaw(raw, { budgetMs = 3000 } = {}) {
    if (this.#decoder) return this.#decoder(raw, budgetMs)
    const cache = this.#env.TELEMETRY_JAVA_CACHE
    if (!cache) throw new Error('TELEMETRY_JAVA_CACHE is required for independent decoding')
    const file = path.join(this.directory, `raw-${sha256(raw)}.bin`)
    try {
      fs.writeFileSync(file, raw, { flag: 'wx', mode: 0o600 })
    } catch (error) {
      if (error.code !== 'EEXIST' || !fs.readFileSync(file).equals(raw)) throw error
    }
    const seconds = (Math.max(budgetMs, 1) / 1000).toFixed(3)
    const child = spawn(
      'timeout',
      ['-k', '1s', `${seconds}s`, 'bash', this.#script, 'decode', this.#line, file],
      {
        env: { ...this.#env, TMPDIR: this.tmp, TELEMETRY_JAVA_CACHE: cache },
        stdio: ['ignore', 'pipe', 'pipe'],
      }
    )
    const memory = new DecoderMemory(child.pid)
    const entry = { pid: child.pid, done: collect(child) }
    this.#children.add(entry)
    const result = await entry.done.finally(() => {
      memory.stop()
      this.#children.delete(entry)
    })
    fs.writeFileSync(
      path.join(this.directory, `decoder-${child.pid}.json`),
      JSON.stringify({
        ...result,
        error: result.error?.message,
        rawSha256: sha256(raw),
        line: this.#line,
        memory: memory.facts(),
      }),
      { flag: 'wx', mode: 0o600 }
    )
    if (result.error && entry.pid === undefined) throw result.error
    if (!(await this.#groupGone(entry.pid))) {
      this.#signalGroup(entry.pid, 'SIGKILL')
      throw new Error('Decoder process group survived its supervisor')
    }
    if (result.error) throw result.error
    if (result.code !== 0)
      throw new Error(
        `Decoder failed (${result.code ?? result.signal}): ${result.stderr.slice(0, 500)}`
      )
    const lines = result.stdout.trim().split('\n')
    if (lines.length !== 2) throw new Error('Decoder must print exactly two JSON lines')
    const [identity, body] = lines.map((line) => JSON.parse(line))
    if (identity.verified !== true || identity.line !== this.#line)
      throw new Error('Decoder identity line is not verified')
    if (body.parse !== 'ok') throw new Error(`Decoder rejected the payload: ${body.parse}`)
    return body
  }

  /**
   * @throws {Error|AggregateError} the first latched fixture failure, or a sink overflow/corruption
   */
  assertHealthy() {
    if (this.#failures.length === 1) throw this.#failures[0]
    if (this.#failures.length > 1)
      throw new AggregateError(this.#failures, 'Telemetry observer failures')
    try {
      this.#reader.readExports()
    } catch (error) {
      this.#latch(error)
      throw error
    }
  }

  /**
   * Detaches every listener and write wrapper, joins decoder children and cancels retry timers.
   * It never destroys a client socket: surviving socket identities stay in the resource ledger.
   * Retains scalar evidence only, releasing socket callbacks and frame parsers after detachment.
   * Idempotent; later calls return the first disposal.
   * @returns {Promise<void>} when no owned child, timer or listener remains
   */
  dispose() {
    this.#disposal ??= this.#dispose()
    return this.#disposal
  }

  /**
   * @param {{allowUnanswered?:string[]}} [options] request keys that were deliberately left
   *   unanswered, such as a final Push cut off by close
   * Functional health only; ownership acceptance is replayed separately in memory-gates.
   * @throws {Error|AggregateError} for a latched failure, missing disposal or an unanswered
   *   request not explicitly allowed by the exact cancellation ledger
   */
  assertFunctional({ allowUnanswered = [] } = {}) {
    const errors = [...this.#failures]
    if (!this.#disposal) errors.push(new Error('The telemetry observer was not disposed'))
    if (
      this.#completeStream &&
      JSON.stringify([...allowUnanswered].sort()) !== JSON.stringify(this.cancellationKeys().sort())
    )
      errors.push(new Error('Cleanup allowance differs from exact cancellation ledger'))
    const unanswered = this.#records.filter(
      (r) => r.type === 'request' && !r.response && !allowUnanswered.includes(r.requestKey)
    )
    if (unanswered.length)
      errors.push(new Error(`Unanswered requests: ${unanswered.map((r) => r.requestKey)}`))
    if (errors.length === 1) throw errors[0]
    if (errors.length) throw new AggregateError(errors, 'The telemetry observer is not clean')
  }

  /**
   * Requires functional health and memory acceptance. Functional lanes call assertFunctional.
   * @param {object} [options] exact cancellation allowance
   * @throws {Error|AggregateError} for functional corruption or surviving owned resources
   */
  assertClean(options) {
    this.assertFunctional(options)
    const errors = this.#stillOpen.map(
      (id) => new Error(`Observed client socket ${id} is still open`)
    )
    if (this.#children.size || this.#timers.size)
      errors.push(new Error('The telemetry observer still owns a child or timer'))
    for (const state of this.#sockets.values())
      if (state.attached) errors.push(new Error(`Socket ${state.id} still has observer listeners`))
    if (errors.length === 1) throw errors[0]
    if (errors.length) throw new AggregateError(errors, 'The telemetry observer is not clean')
  }

  /**
   * @param {Error} error fixture failure to retain, never replacing foreground behavior
   */
  #latch(error) {
    this.#failures.push(error instanceof Error ? error : new Error(String(error)))
  }

  /**
   * @param {string} kind socket classification
   * @returns {object[]} summaries of sockets with that classification
   */
  #socketsOf(kind) {
    return [...this.#sockets.values()]
      .filter((state) => state.kind === kind)
      .map(({ id, node, port }) => ({ socketId: id, node, port }))
  }

  /**
   * @param {string|undefined} uuid optional stream
   * @param {number} after transcript cursor
   * @param {boolean} terminating which Push kind
   * @returns {object[]} public Push summaries with error 0 responses
   */
  #pushes(uuid, after, terminating) {
    const wanted = uuid === undefined ? undefined : canonicalUuid(uuid)
    return this.#records
      .filter(
        (r) =>
          r.type === 'request' &&
          r.apiKey === 72 &&
          r.index > after &&
          r.apiVersion === 0 &&
          r.terminating === terminating &&
          r.response?.errorCode === 0 &&
          (wanted === undefined || r.uuid === wanted)
      )
      .map((r) => ({
        requestKey: r.requestKey,
        index: r.index,
        socketId: r.socketId,
        node: r.node,
        uuid: r.uuid,
        version: r.apiVersion,
        errorCode: r.response.errorCode,
        terminating: r.terminating,
        compressionType: r.compressionType,
        subscriptionId: r.subscriptionId,
        clientId: r.clientId,
        writeAt: r.writeAt,
        responseAt: r.response.responseAt,
        metricsBytes: r.metricsBytes,
      }))
  }

  /**
   * Starts observing one dialed socket; a bound hit latches and leaves the socket unobserved.
   * @param {import('net').Socket} socket the real or fake socket
   * @param {number} port requested fixture port
   * @param {string} node broker node of that port
   */
  #attach(socket, port, node) {
    if (this.#sockets.size >= this.#limits.sockets) {
      this.#latch(new Error('Observer socket bound exceeded'))
      return
    }
    const observer = this
    const original = socket.write
    const state = {
      id: this.#sockets.size + 1,
      port,
      node,
      kind: 'undecided',
      closed: false,
      attached: true,
      pending: new Map(),
      dead: { out: false, in: false },
    }
    const stream = (onFrame) =>
      new FrameStream(this.#limits, onFrame, (error) => this.#latch(error))
    state.out = stream((frame) => this.#request(state, frame))
    state.in = stream((frame) => this.#response(state, frame))
    state.onData = (chunk) => this.#capture(state, 'in', chunk)
    state.onConnect = () => this.#connected(state, socket)
    let resolveClosed
    state.closedPromise = new Promise((resolve) => {
      resolveClosed = resolve
    })
    state.onClose = () => {
      state.closed = true
      state.closedAt = this.#clock.now()
      resolveClosed()
    }
    state.detach = () => {
      socket.removeListener('data', state.onData)
      socket.removeListener('connect', state.onConnect)
      socket.removeListener('close', state.onClose)
      if (state.ownWrite) socket.write = original
      else delete socket.write
      state.attached = false
    }
    state.ownWrite = Object.hasOwn(socket, 'write')
    socket.write = function (...args) {
      observer.#capture(state, 'out', args[0], typeof args[1] === 'string' ? args[1] : undefined)
      return Reflect.apply(original, this, args)
    }
    socket.on('data', state.onData)
    socket.once('connect', state.onConnect)
    socket.once('close', state.onClose)
    this.#sockets.set(state.id, state)
  }

  /**
   * Feeds observed bytes into a direction's frame parser without ever throwing.
   * @param {object} state socket state
   * @param {'in'|'out'} direction stream direction
   * @param {Buffer|string|Uint8Array} chunk bytes as written or received
   * @param {string} [encoding] encoding of a string chunk
   */
  #capture(state, direction, chunk, encoding) {
    if (state.dead[direction]) return
    try {
      const bytes =
        typeof chunk === 'string'
          ? Buffer.from(chunk, encoding)
          : Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength)
      state[direction].push(bytes)
    } catch (error) {
      state.dead[direction] = true
      this.#latch(error)
    }
  }

  /**
   * Records a successful physical connect after verifying the actual peer.
   * @param {object} state socket state
   * @param {import('net').Socket} socket connected socket
   */
  #connected(state, socket) {
    try {
      if (socket.remoteAddress !== PEER_ADDRESS || Number(socket.remotePort) !== state.port)
        throw new Error(
          `Socket ${state.id} connected to ${socket.remoteAddress}:${socket.remotePort}, expected ${PEER_ADDRESS}:${state.port}`
        )
      this.#add({
        type: 'connect',
        socketId: state.id,
        node: state.node,
        port: state.port,
        localAddress: socket.localAddress,
        localPort: socket.localPort,
        remoteAddress: socket.remoteAddress,
        remotePort: socket.remotePort,
        at: this.#clock.now(),
      })
    } catch (error) {
      this.#latch(error)
    }
  }

  /**
   * @param {object} record transcript entry without an index
   * @returns {object} the stored entry
   * @throws {Error} at the record bound
   */
  #add(record) {
    if (this.#records.length >= this.#limits.records)
      throw new Error('Observer record bound exceeded')
    this.#retain(64)
    record.index = this.#records.length + 1
    this.#records.push(record)
    return record
  }

  /**
   * @param {number} bytes retained wire bytes to account
   * @throws {Error} at the retained-bytes bound
   */
  #retain(bytes) {
    this.#retained += bytes
    if (this.#retained > this.#limits.retained)
      throw new Error('Observer retained-byte bound exceeded')
  }

  /**
   * @param {object} state socket state
   * @param {{length:number,prefix:Buffer,sha256:string}} frame completed outbound frame
   */
  #request(state, frame) {
    const head = parseRequest(frame.prefix, frame.length)
    if (state.pending.has(head.correlation))
      throw new Error(`Duplicate pending correlation ${head.correlation} on socket ${state.id}`)
    if (state.pending.size >= this.#limits.pending)
      throw new Error('Observer pending bound exceeded')
    const { blob, ...fields } = head
    const record = {
      type: 'request',
      requestKey: `r${this.#records.length + 1}`,
      socketId: state.id,
      node: state.node,
      owner: this.#owner,
      ...fields,
      size: frame.length,
      sha256: frame.sha256,
      writeAt: this.#clock.now(),
    }
    if (blob) {
      this.#retain(blob.length)
      record.metricsBytes = blob.length
      record.metricsSha256 = sha256(blob)
    }
    this.#add(record)
    if (blob) {
      fs.writeFileSync(path.join(this.directory, `push-${record.requestKey}.bin`), blob, {
        flag: 'wx',
        mode: 0o600,
      })
      this.#blobs.set(record.requestKey, blob)
    }
    state.pending.set(head.correlation, record)
    if (TELEMETRY_APIS.has(head.apiKey)) state.kind = 'telemetry'
    else if (state.kind === 'undecided' && !SETUP_APIS.has(head.apiKey)) state.kind = 'normal'
  }

  /**
   * @param {object} state socket state
   * @param {{length:number,prefix:Buffer,sha256:string}} frame completed inbound frame
   */
  #response(state, frame) {
    if (frame.prefix.length < 4) throw new Error('Response shorter than a correlation id')
    const correlation = frame.prefix.readInt32BE(0)
    const record = state.pending.get(correlation)
    if (!record)
      throw new Error(`Unexpected response correlation ${correlation} on socket ${state.id}`)
    state.pending.delete(correlation)
    const parsed = [18, 71, 72].includes(record.apiKey)
      ? parseResponse(frame.prefix, frame.length, record)
      : {}
    record.response = {
      responseAt: this.#clock.now(),
      size: frame.length,
      sha256: frame.sha256,
      ...parsed,
    }
    if (record.apiKey === 18 && parsed.ranges) state.ranges = parsed.ranges
    if (
      this.#completeStream &&
      record.apiKey === 72 &&
      parsed.errorCode === 0 &&
      record.metricsBytes > 0
    )
      this.#captureTail = this.#captureTail
        .then(() => this.decodeAccepted(record))
        .catch((error) => this.#latch(error))
  }

  /**
   * Finds the next unconsumed sink record that matches a captured Push.
   * @param {object} record captured request
   * @param {number} rawBytes decompressed length
   * @param {string} rawSha256 decompressed hash
   * @param {number} waitMs how long the broker sink may lag the response
   * @returns {Promise<object>} the consumed sink record
   * @throws {Error} when none matches within the wait
   */
  async #join(record, rawBytes, rawSha256, waitMs) {
    const attempts = Math.ceil(waitMs / 50)
    for (let attempt = 0; ; attempt++) {
      const row = this.#reader
        .readExports({ uuid: record.uuid })
        .find(
          (r) =>
            r.node === record.node &&
            r.dataBytes === rawBytes &&
            r.dataSha256 === rawSha256 &&
            r.terminating === record.terminating &&
            (r.clientId == null || r.clientId === record.clientId) &&
            !this.#consumed.has(`${r.node}:${r.seq}`)
        )
      if (row) {
        this.#consumed.add(`${row.node}:${row.seq}`)
        return row
      }
      if (attempt >= attempts)
        throw new Error(`No unconsumed sink record matches Push ${record.requestKey}`)
      await this.#sleep(50)
      if (this.#blobs.size === 0) throw new Error('The telemetry observer was disposed')
    }
  }

  /**
   * @param {number} ms delay, released early by `dispose`
   * @returns {Promise<void>} after the delay
   */
  #sleep(ms) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.#timers.delete(timer)
        resolve()
      }, ms)
      this.#timers.set(timer, resolve)
    })
  }

  /**
   * @param {number} pid decoder supervisor, which leads its own process group
   * @returns {Promise<boolean>} whether the whole group has exited within one second
   */
  async #groupGone(pid) {
    for (let attempt = 0; attempt < 20; attempt++) {
      try {
        process.kill(-pid, 0)
      } catch (error) {
        return error.code === 'ESRCH'
      }
      await this.#sleep(50)
    }
    return false
  }

  /**
   * @param {number} pid group leader
   * @param {string} signal signal name
   */
  #signalGroup(pid, signal) {
    try {
      process.kill(-pid, signal)
    } catch {
      // the group already exited
    }
  }

  /** @returns {Promise<void>} the disposal body */
  async #dispose() {
    if (this.#closures.length && [...this.#sockets.values()].some((s) => !s.closed)) {
      await new Promise((resolve) => {
        const timer = setTimeout(() => {
          this.#timers.delete(timer)
          resolve()
        }, 1000)
        this.#timers.set(timer, resolve)
        Promise.all([...this.#sockets.values()].map((s) => s.closedPromise)).then(() => {
          clearTimeout(timer)
          this.#timers.delete(timer)
          resolve()
        })
      })
    }
    await this.#captureTail
    for (const record of this.#records.filter(
      (r) =>
        this.#completeStream && r.apiKey === 72 && r.response?.errorCode === 0 && r.metricsBytes > 0
    )) {
      try {
        await this.decodeAccepted(record)
      } catch (error) {
        this.#latch(error)
      }
    }
    for (const record of this.#records.filter(
      (r) => r.type === 'request' && TELEMETRY_APIS.has(r.apiKey) && !r.response
    )) {
      const socket = this.#sockets.get(record.socketId)
      const close = this.#closures.find(
        (c) =>
          c.pending.includes(record.requestKey) ||
          (record.apiKey === 72 &&
            record.terminating &&
            record.writeAt >= c.at &&
            record.writeAt <= c.end)
      )
      const fault = this.#faults.find(
        (f) => f.socketId === record.socketId && record.writeAt >= f.at
      )
      if (!socket.closed || (!close && !fault)) continue
      if (
        fault &&
        this.#records.filter(
          (r) =>
            r.type === 'request' &&
            TELEMETRY_APIS.has(r.apiKey) &&
            r.socketId === fault.socketId &&
            r.writeAt >= fault.at
        ).length > 1
      ) {
        this.#latch(new Error('Fault covered more than one telemetry exchange'))
        continue
      }
      this.#cancellations.push({
        requestKey: record.requestKey,
        apiKey: record.apiKey,
        socketId: record.socketId,
        writeAt: record.writeAt,
        at: fault?.at ?? close.at,
        end: socket.closedAt,
        disposition: fault
          ? 'faulted-transport'
          : record.terminating
            ? 'close-final'
            : 'close-in-flight',
      })
    }
    for (const [timer, resolve] of this.#timers) {
      clearTimeout(timer)
      resolve()
    }
    this.#timers.clear()
    for (const child of this.#children) this.#signalGroup(child.pid, 'SIGTERM')
    await Promise.allSettled([...this.#children].map((child) => child.done))
    for (const [id, state] of this.#sockets) {
      if (!state.closed) {
        this.#stillOpen.push(state.id)
      }
      state.detach()
      this.#sockets.set(id, {
        id,
        port: state.port,
        node: state.node,
        kind: state.kind,
        closed: state.closed,
        closedAt: state.closedAt,
        attached: false,
        foregroundBootstrap: state.foregroundBootstrap,
        ranges: state.ranges,
      })
    }
    try {
      fs.writeFileSync(
        path.join(this.directory, 'transcript.json'),
        JSON.stringify({
          owner: this.#owner,
          closures: this.#closures,
          faults: this.#faults,
          cancellations: this.#cancellations,
          records: this.#records,
          sockets: [...this.#sockets.values()].map(
            ({ id, kind, node, foregroundBootstrap, closedAt }) => ({
              id,
              kind,
              node,
              foregroundBootstrap: !!foregroundBootstrap,
              closedAt,
            })
          ),
          cleanup: {
            children: this.#children.size,
            timers: this.#timers.size,
            stillOpen: this.#stillOpen,
            attached: [...this.#sockets.values()].filter((s) => s.attached).length,
          },
          failures: this.#failures.map((e) => e.message),
        })
      )
    } catch (error) {
      this.#latch(error)
    }
    this.#blobs.clear()
  }
}

/**
 * @param {object} record transcript entry
 * @returns {object} a plain JSON copy that shares nothing with the observer
 */
const copy = (record) => JSON.parse(JSON.stringify(record))

/**
 * Collects a supervised child's output; resolves, never rejects, after both pipes closed.
 * @param {import('child_process').ChildProcess} child spawned decoder supervisor
 * @returns {Promise<{code:number|null,signal:string|null,stdout:string,stderr:string,error?:Error}>}
 */
const collect = (child) =>
  new Promise((resolve) => {
    const result = { stdout: '', stderr: '' }
    const cap = 1048576
    child.stdout.setEncoding('utf8')
    child.stderr.setEncoding('utf8')
    child.stdout.on('data', (data) => {
      if (result.stdout.length < cap) result.stdout += data
    })
    child.stderr.on('data', (data) => {
      if (result.stderr.length < cap) result.stderr += data
    })
    child.once('error', (error) => resolve({ ...result, error }))
    child.once('close', (code, signal) => resolve({ ...result, code, signal }))
  })

/**
 * @param {number} pid a process
 * @returns {{name:string, hwm:number, rss:number}|null} its name and kernel high-water and current
 *   resident bytes from `/proc/<pid>/status`, or null when unreadable or without either field
 */
const readPidStatus = (pid) => {
  try {
    const text = fs.readFileSync(`/proc/${pid}/status`, 'utf8')
    const kib = (field) => {
      const match = new RegExp(`^${field}:\\s+(\\d+) kB$`, 'm').exec(text)
      return match ? Number(match[1]) * 1024 : null
    }
    const hwm = kib('VmHWM')
    const rss = kib('VmRSS')
    if (hwm === null || rss === null) return null
    return { name: /^Name:\s+(.*)$/m.exec(text)?.[1] ?? '', hwm, rss }
  } catch {
    return null
  }
}

/**
 * @param {number} pid a process
 * @returns {number[]|null} its children from `/proc/<pid>/task/<pid>/children`, or null when the
 *   kernel offers no such list for it
 */
const readPidChildren = (pid) => {
  try {
    return fs
      .readFileSync(`/proc/${pid}/task/${pid}/children`, 'utf8')
      .split(/\s+/)
      .filter(Boolean)
      .map(Number)
  } catch {
    return null
  }
}

/**
 * The separately reported memory of one decoder child, kept out of the measured worker. While the
 * child runs it samples `VmHWM` and `VmRSS` of the supervisor and every descendant (the Java
 * decoder among them) every 20 ms, and reports the largest single-process high water, the largest
 * summed resident size of one sample and the few largest processes. Where the kernel offers no
 * status or child list the fact says `unavailable` instead of claiming a measurement. It is a
 * report, never a gate or a subtraction.
 */
class DecoderMemory {
  /** The labels every fact carries. */
  static LABEL = Object.freeze({ source: 'proc-pid-status', units: 'bytes' })
  /** The most processes described one by one, the largest high waters kept. */
  static PROCESSES = 8

  #timer = null
  #processes = new Map()
  #samples = 0
  #peakRss = 0
  #peakHwm = 0
  #walked = false

  /**
   * Starts sampling at once; `stop` ends it.
   * @param {number|undefined} pid the decoder supervisor, which leads its process group
   * @param {object} [options] seams for controls
   * @param {function(number):object|null} [options.read] reads one process status
   * @param {function(number):number[]|null} [options.children] lists one process's children
   * @param {number} [options.intervalMs] sampling period
   */
  constructor(pid, { read = readPidStatus, children = readPidChildren, intervalMs = 20 } = {}) {
    Object.assign(this, { pid, read, children, intervalMs })
    if (!Number.isInteger(pid)) return
    this.sample()
    this.#timer = setInterval(() => this.sample(), intervalMs)
    this.#timer.unref?.()
  }

  /**
   * @param {string} reason why nothing could be read
   * @returns {object} the explicit fact of a decoder child whose memory could not be read
   */
  static unavailable(reason) {
    return {
      ...DecoderMemory.LABEL,
      capability: 'unavailable',
      reason,
      intervalMs: null,
      samples: 0,
      peakHwmBytes: null,
      peakRssBytes: null,
      processes: [],
    }
  }

  /** @returns {void} reads every process of the tree once, at most 64 of them */
  sample() {
    const queue = [this.pid]
    let rss = 0
    let seen = false
    for (let visited = 0; queue.length && visited < 64; visited++) {
      const pid = queue.shift()
      const kids = this.children(pid)
      if (kids) {
        if (pid === this.pid) this.#walked = true
        queue.push(...kids)
      }
      const status = this.read(pid)
      if (!status) continue
      seen = true
      rss += status.rss
      this.#peakHwm = Math.max(this.#peakHwm, status.hwm)
      const known = this.#processes.get(pid) ?? { pid, name: status.name, hwmBytes: 0, rssBytes: 0 }
      known.hwmBytes = Math.max(known.hwmBytes, status.hwm)
      known.rssBytes = Math.max(known.rssBytes, status.rss)
      this.#processes.set(pid, known)
      if (this.#processes.size > DecoderMemory.PROCESSES) {
        const smallest = [...this.#processes.values()].reduce((a, b) =>
          b.hwmBytes < a.hwmBytes ? b : a
        )
        this.#processes.delete(smallest.pid)
      }
    }
    if (!seen) return
    this.#samples++
    this.#peakRss = Math.max(this.#peakRss, rss)
  }

  /** @returns {void} ends sampling; idempotent */
  stop() {
    clearInterval(this.#timer)
    this.#timer = null
  }

  /** @returns {object} the labelled peak fact, or the explicit `unavailable` one */
  facts() {
    if (!this.#samples) return DecoderMemory.unavailable('no process status could be read')
    if (!this.#walked) return DecoderMemory.unavailable('the process tree could not be listed')
    return {
      ...DecoderMemory.LABEL,
      capability: 'available',
      intervalMs: this.intervalMs,
      samples: this.#samples,
      peakHwmBytes: this.#peakHwm,
      peakRssBytes: this.#peakRss,
      processes: [...this.#processes.values()]
        .sort((a, b) => b.hwmBytes - a.hwmBytes)
        .map((p) => ({ ...p })),
    }
  }
}

/**
 * Audits every built-in descriptor and node window against independently joined normal requests.
 * Bootstrap requests stay in the ledger but cannot introduce a node. Each observed known-node
 * completion must occur in exactly one compatible, ordered Push window; idle windows have no gauge.
 * @param {object} input kind, decoded samples, joined requests, physical connects and temporality
 * @returns {object} exact normal identities, connection arithmetic and node/window allocations
 * @throws {Error} on a missing/duplicate descriptor, fabricated node, omission or incorrect value
 */
const verifyMetricWindows = ({ kind, samples, requests, connects, delta }) => {
  const fail = (message) => {
    throw new Error(message)
  }
  const prefix = `org.apache.kafka.${kind === 'share' ? 'consumer' : kind}.`
  const names = [
    'connection.creation.total',
    'node.request.latency.avg',
    'node.request.latency.max',
  ]
  if (new Set(requests.map((r) => r.requestKey)).size !== requests.length)
    fail('Duplicate normal request identity')
  for (const request of requests)
    if (!Number.isFinite(request.duration) || request.duration < 0)
      fail('Invalid observed duration')
  const gauges = []
  const sums = []
  for (const sample of samples) {
    const seen = new Set()
    const nodes = [{}, {}]
    for (const metric of sample.decoded.metrics) {
      const name = metric.name.slice(prefix.length)
      if (!metric.name.startsWith(prefix) || !names.includes(name)) fail('Unexpected descriptor')
      if (seen.has(name)) fail('Duplicate descriptor')
      seen.add(name)
      if (name === names[0]) {
        if (metric.points.length !== 1 || metric.points[0].attrs.length)
          fail('Invalid connection point')
        sums.push(BigInt(metric.points[0].asInt))
      } else {
        const map = nodes[name === names[1] ? 0 : 1]
        if (!metric.points.length) fail('Empty gauge descriptor')
        for (const point of metric.points) {
          const [attr] = point.attrs
          if (point.attrs.length !== 1 || attr[0] !== 'node_id' || attr[1] !== 'string')
            fail('Invalid node identity')
          if (Object.hasOwn(map, attr[2])) fail('Duplicate node point')
          const value = Number(point.asDouble)
          if (!Number.isFinite(value) || value < 0) fail('Invalid gauge value')
          map[attr[2]] = value
        }
      }
    }
    if (!seen.has(names[0])) fail('Missing connection descriptor')
    if (
      JSON.stringify(Object.keys(nodes[0]).sort()) !== JSON.stringify(Object.keys(nodes[1]).sort())
    )
      fail('Mismatched node identity pairs')
    gauges.push(nodes)
  }
  if (!samples.length) fail('Missing metric samples')
  const expectedNodes = [...new Set(requests.filter((r) => r.knownNode).map((r) => r.node))].sort()
  const emittedNodes = [...new Set(gauges.flatMap(([nodes]) => Object.keys(nodes)))].sort()
  if (JSON.stringify(expectedNodes) !== JSON.stringify(emittedNodes))
    fail('Unexpected or missing node identity')
  const allocations = expectedNodes.map((node) => {
    const events = requests.filter((r) => r.knownNode && r.node === node)
    const visit = (window, offset) => {
      if (window === samples.length) return offset === events.length ? [] : null
      const [avg, max] = gauges[window]
      const previousWrite = window === 0 ? -Infinity : samples[window - 1].writeAt
      if (!Object.hasOwn(avg, node)) {
        if (events[offset]?.responseAt <= previousWrite) return null
        const rest = visit(window + 1, offset)
        return rest && [[], ...rest]
      }
      for (let end = offset + 1; end <= events.length; end++) {
        const selected = events.slice(offset, end)
        if (selected.some((r) => r.responseAt > samples[window].writeAt)) break
        const durations = selected.map((r) => r.duration)
        const mean = durations.reduce((a, b) => a + b, 0) / durations.length
        if (
          Math.abs(avg[node] - mean) > 1e-9 + 1e-12 * Math.abs(mean) ||
          max[node] !== Math.max(...durations)
        )
          continue
        if (events[end]?.responseAt <= previousWrite) continue
        const rest = visit(window + 1, end)
        if (rest) return [selected.map((r) => r.requestKey), ...rest]
      }
      return null
    }
    const windows = visit(0, 0)
    if (!windows) fail('Node request arithmetic does not account for every completion')
    return { node, windows }
  })
  const total = delta ? sums.reduce((a, b) => a + b, 0n) : sums[sums.length - 1]
  const expected = connects.filter((c) => c.at < samples[samples.length - 1].writeAt).length
  if (total !== BigInt(expected)) fail('Connection arithmetic mismatch')
  let cumulative = 0n
  for (let i = 0; i < samples.length; i++) {
    cumulative = delta ? cumulative + sums[i] : sums[i]
    if (
      sums[i] < 0n ||
      (!delta && i > 0 && sums[i] < sums[i - 1]) ||
      cumulative > BigInt(connects.filter((c) => c.at < samples[i].writeAt).length)
    )
      fail('Connection window exceeds observed successes')
  }
  return {
    requests,
    connects,
    allocations,
    sums: sums.map(String),
    total: String(total),
    expected,
    delta,
  }
}

module.exports = {
  readCreatedTopicConfig,
  canonicalUuid,
  checkedResource,
  DecoderGate,
  DecoderMemory,
  OBSERVER_LIMITS: DEFAULT_LIMITS,
  verifyMetricWindows,
  TelemetryExportReader,
  TelemetryWireObserver,
  readExports,
  withTelemetryResource,
}
