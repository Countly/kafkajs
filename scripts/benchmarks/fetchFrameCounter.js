/**
 * Counts complete Kafka Fetch frames on a single measurement window.
 * Connection keys include reconnect identity; response IDs are never global.
 * @class
 */
class FetchFrameCounter {
  /**
   * @param {number} [maxFrameBytes] rejects invalid unbounded frame declarations
   * @param {Function} [onFrame] primary complete-frame observer, before state is released
   * @returns {FetchFrameCounter} empty observer
   * @throws Never for the default bound.
   */
  constructor(maxFrameBytes = 134217728, onFrame = () => {}) {
    this.onFrame = onFrame
    this.maxFrameBytes = maxFrameBytes
    this.connections = new Map()
    this.requests = 0
    this.responses = 0
    this.bytes = 0
  }

  /**
   * Consumes request or response chunks, including fragmented or coalesced frames.
   * @param {string} connection unique socket identity
   * @param {'request'|'response'} direction observation direction
   * @param {Buffer} chunk copied bytes from the socket adapter
   * @param {number} [at] chunk observation time from the shared monotonic clock
   * @returns {void} counts Fetch only; tracks other APIs for response exclusion
   * @throws {Error} malformed frame, duplicate request or uncorrelated response
   */
  push(connection, direction, chunk, at = performance.now()) {
    let state = this.connections.get(connection)
    if (!state) {
      state = {
        request: Buffer.alloc(0),
        response: Buffer.alloc(0),
        pending: new Map(),
      }
      this.connections.set(connection, state)
    }
    state[direction] = Buffer.concat([state[direction], chunk])
    while (state[direction].length >= 4) {
      const bytes = state[direction]
      const length = bytes.readInt32BE(0)
      const minimum = direction === 'request' ? 10 : 4
      if (length < minimum || length + 4 > this.maxFrameBytes)
        throw new Error('Invalid frame length')
      if (bytes.length < length + 4) return
      const frame = bytes.subarray(0, length + 4)
      state[direction] = bytes.subarray(length + 4)
      const id = frame.readInt32BE(direction === 'request' ? 8 : 4)
      let api, apiVersion
      if (direction === 'request') {
        if (state.pending.has(id)) throw new Error('Duplicate correlation ID')
        api = frame.readInt16BE(4)
        apiVersion = frame.readInt16BE(6)
        state.pending.set(id, { api, apiVersion })
      } else {
        if (!state.pending.has(id)) throw new Error('Uncorrelated response')
        ;({ api, apiVersion } = state.pending.get(id))
        state.pending.delete(id)
      }
      if (api === 1) {
        this.onFrame({
          connection,
          direction,
          at,
          bytes: frame.length,
          correlationId: id,
          apiKey: api,
          apiVersion,
        })
        this.bytes += frame.length
        if (direction === 'request') this.requests++
        else this.responses++
      }
    }
  }

  /**
   * Returns a complete-window result, refusing partial frames or outstanding replies.
   * @returns {{bytes:number,requests:number,responses:number}} independent byte counter
   * @throws {Error} incomplete frame or unmatched request
   */
  result() {
    for (const state of this.connections.values()) {
      if (state.request.length || state.response.length || state.pending.size)
        throw new Error('Incomplete observation window')
    }
    return {
      bytes: this.bytes,
      requests: this.requests,
      responses: this.responses,
    }
  }
}

/**
 * Independently parses raw socket chunks after a window, without the counter parser.
 * The adapter records chunks before calling the primary observer. Non-Fetch APIs
 * correlate but contribute no bytes or cycle membership. IDs are scoped by socket.
 * @param {{connection:string,nodeId:number,direction:string,chunk:Buffer,at?:number}[]} transcript
 * @param {{sessions?:boolean}} [options] decode actual session headers for real frames
 * @returns {{bytes:number,requests:number,responses:number,frames:object[]}} frame evidence
 * @throws {Error} malformed, partial, duplicate, unmatched or unknown reply
 */
function auditTranscript(transcript, { sessions = false } = {}) {
  const sockets = new Map()
  for (const entry of transcript) {
    let socket = sockets.get(entry.connection)
    if (!socket) {
      socket = { nodeId: entry.nodeId, request: [], response: [] }
      sockets.set(entry.connection, socket)
    }
    if (socket.nodeId !== entry.nodeId) throw new Error('Connection changed node')
    socket[entry.direction].push(entry.chunk)
  }
  const frames = []
  for (const [connection, socket] of sockets) {
    const completed = []
    for (const direction of ['request', 'response']) {
      const chunks = transcript
        .map((entry, sequence) => ({ ...entry, sequence }))
        .filter((entry) => entry.connection === connection && entry.direction === direction)
      const bytes = Buffer.concat(socket[direction])
      let boundary = 0
      let chunkIndex = 0
      for (let offset = 0; offset < bytes.length;) {
        if (bytes.length - offset < 4) throw new Error('Partial transcript prefix')
        const size = bytes.readInt32BE(offset) + 4
        if (
          size < (direction === 'request' ? 14 : 8) ||
          size > 134217728 ||
          offset + size > bytes.length
        )
          throw new Error('Invalid transcript frame')
        while (boundary < offset + size) boundary += chunks[chunkIndex++].chunk.length
        completed.push({
          connection,
          nodeId: socket.nodeId,
          direction,
          bytes: size,
          correlationId: bytes.readInt32BE(offset + (direction === 'request' ? 8 : 4)),
          apiKey: direction === 'request' ? bytes.readInt16BE(offset + 4) : undefined,
          apiVersion: direction === 'request' ? bytes.readInt16BE(offset + 6) : undefined,
          sequence: chunks[chunkIndex - 1].sequence,
          offset,
          at: chunks[chunkIndex - 1].at,
          raw: bytes.subarray(offset, offset + size),
        })
        offset += size
      }
    }
    const pending = new Map()
    completed.sort((a, b) => a.sequence - b.sequence || a.offset - b.offset)
    for (const frame of completed) {
      const id = frame.correlationId
      let request
      if (frame.direction === 'request') {
        if (pending.has(id)) throw new Error('Duplicate transcript ID')
        request = { apiKey: frame.apiKey, apiVersion: frame.apiVersion }
        pending.set(id, request)
      } else {
        request = pending.get(id)
        if (!request) throw new Error('Unknown transcript response')
        pending.delete(id)
      }
      if (request.apiKey === 1) {
        const { raw, ...metadata } = frame
        frames.push({
          ...metadata,
          ...request,
          ...(sessions
            ? fetchSessionHeader(raw, {
                direction: frame.direction,
                apiVersion: request.apiVersion,
              })
            : {}),
        })
      }
    }
    if (pending.size) throw new Error('Missing transcript response')
  }
  return {
    bytes: frames.reduce((sum, frame) => sum + frame.bytes, 0),
    requests: frames.filter((frame) => frame.direction === 'request').length,
    responses: frames.filter((frame) => frame.direction === 'response').length,
    frames,
  }
}

/**
 * Refuses a primary observation that differs from independently parsed frames.
 * @param {{bytes:number,requests:number,responses:number}} actual primary result
 * @param {object} transcript independent audit result
 * @returns {void} equal Fetch counts
 * @throws {Error} any mismatch
 */
function verifyFrames(actual, transcript) {
  for (const key of ['bytes', 'requests', 'responses']) {
    if (actual[key] !== transcript[key])
      throw new Error(`Fetch ${key}: expected ${transcript[key]}, received ${actual[key]}`)
  }
}

/**
 * Reads only independent Kafka/Fetch headers, never record decoding or client internals.
 * Flexible request/response header tags are skipped with checked bounds. Unknown
 * versions refuse measurement instead of applying a guessed body offset.
 * @param {Buffer} frame complete length-prefixed request or response
 * @param {{direction:string,apiVersion:number}} options correlated request version
 * @returns {object} actual session ID, epoch and full/incremental/fallback classification
 * @throws {Error} malformed frame or unsupported version
 */
function fetchSessionHeader(frame, { direction, apiVersion }) {
  if (apiVersion < 4 || apiVersion > 18) throw new Error('Unsupported Fetch observation version')
  let offset = 4
  const take = (count) => {
    if (!Number.isSafeInteger(count) || count < 0 || offset + count > frame.length)
      throw new Error('Truncated Fetch observation')
    const start = offset
    offset += count
    return start
  }
  const uv = () => {
    let value = 0
    let multiplier = 1
    for (let count = 0; count < 5; count++) {
      const byte = frame[take(1)]
      value += (byte & 127) * multiplier
      if (byte < 128) {
        if (value > 4294967295) break
        return value
      }
      multiplier *= 128
    }
    throw new Error('Invalid Fetch observation varint')
  }
  const tags = () => {
    const count = uv()
    for (let index = 0; index < count; index++) {
      uv()
      take(uv())
    }
  }
  if (frame.length < 8 || frame.readInt32BE(0) !== frame.length - 4)
    throw new Error('Invalid Fetch observation size')
  if (direction === 'request') {
    take(8)
    const clientLength = frame.readInt16BE(take(2))
    if (clientLength < -1) throw new Error('Invalid Fetch client ID')
    take(Math.max(0, clientLength))
    if (apiVersion >= 12) tags()
    if (apiVersion <= 14) take(4)
    take(13)
    if (apiVersion < 7) return { sessionId: 0, sessionEpoch: -1, kind: 'sessionless' }
    const sessionId = frame.readInt32BE(take(4))
    const sessionEpoch = frame.readInt32BE(take(4))
    const kind =
      sessionEpoch === -1
        ? sessionId > 0
          ? 'close'
          : 'sessionless'
        : sessionEpoch === 0
          ? 'full'
          : 'incremental'
    const flexible = apiVersion >= 12
    const array = () => {
      const count = flexible ? uv() - 1 : frame.readInt32BE(take(4))
      if (count < 0 || count > frame.length) throw new Error('Invalid Fetch observation array')
      return count
    }
    const topic = () => {
      if (apiVersion >= 13) {
        take(16)
        return
      }
      const length = flexible ? uv() - 1 : frame.readInt16BE(take(2))
      take(length)
    }
    let partitions = 0
    let forgottenPartitions = 0
    const topics = array()
    for (let index = 0; index < topics; index++) {
      topic()
      const count = array()
      partitions += count
      for (let partition = 0; partition < count; partition++) {
        take(24 + (apiVersion >= 9 ? 4 : 0) + (flexible ? 4 : 0))
        if (flexible) tags()
      }
      if (flexible) tags()
    }
    const forgotten = array()
    for (let index = 0; index < forgotten; index++) {
      topic()
      const count = array()
      forgottenPartitions += count
      take(count * 4)
      if (flexible) tags()
    }
    if (apiVersion >= 11) take(flexible ? uv() - 1 : frame.readInt16BE(take(2)))
    if (flexible) tags()
    if (offset !== frame.length) throw new Error('Trailing Fetch observation bytes')
    return { sessionId, sessionEpoch, kind, partitions, forgottenPartitions }
  }
  if (direction !== 'response') throw new Error('Invalid Fetch observation direction')
  take(4)
  if (apiVersion >= 12) tags()
  const throttleTime = frame.readInt32BE(take(4))
  if (apiVersion < 7) return { sessionId: 0, kind: 'sessionless', throttleTime }
  const errorCode = frame.readInt16BE(take(2))
  const sessionId = frame.readInt32BE(take(4))
  return {
    throttleTime,
    errorCode,
    sessionId,
    kind: errorCode ? 'rejected' : sessionId === 0 ? 'fallback' : 'session',
  }
}

/**
 * Counts records and actual callback timestamps for one handler arm. One instance belongs to one
 * run; it adds no clocks or per-record objects to the inherited handler loop.
 */
class DeliveryAccounting {
  /** @param {'eachBatch'|'eachMessage'} handler selected public callback @returns {DeliveryAccounting} empty counts @throws Never. */
  constructor(handler) {
    this.handler = handler
    this.messages = 0
    this.batches = 0
    this.entries = []
  }

  /** @param {number} records actual batch length, or one message @param {bigint} at entry timestamp @returns {void} @throws Never for normalized counts. */
  enter(records, at) {
    this.entries.push(at)
    this.messages += records
    if (this.handler === 'eachBatch') this.batches += 1
  }

  /** @returns {object} independent message, batch, entry and actual-gap measurements @throws Never. */
  result() {
    const gaps = this.entries
      .slice(1)
      .map((entered, index) => Number(entered - this.entries[index]) / 1e6)
    const sorted = [...gaps].sort((a, b) => a - b)
    const at = (p) =>
      sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] : null
    const latency = { p50: at(0.5), p90: at(0.9), p99: at(0.99) }
    return {
      deliveredMessages: this.messages,
      deliveredBatches: this.batches,
      handlerEntries: this.entries.length,
      gapSamples: gaps.length,
      deliveryGapP50: latency.p50,
      deliveryGapP90: latency.p90,
      deliveryGapP99: latency.p99,
    }
  }
}

module.exports = {
  FetchFrameCounter,
  auditTranscript,
  verifyFrames,
  fetchSessionHeader,
  DeliveryAccounting,
}
