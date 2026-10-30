/**
 * Counts complete Kafka Fetch frames on a single measurement window.
 * Connection keys include reconnect identity; response IDs are never global.
 * @class
 */
class FetchFrameCounter {
  /**
   * @param {number} [maxFrameBytes] rejects invalid unbounded frame declarations
   * @returns {FetchFrameCounter} empty observer
   * @throws Never for the default bound.
   */
  constructor(maxFrameBytes = 134217728) {
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
   * @returns {void} counts Fetch only; tracks other APIs for response exclusion
   * @throws {Error} malformed frame, duplicate request or uncorrelated response
   */
  push(connection, direction, chunk) {
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
      let api
      if (direction === 'request') {
        if (state.pending.has(id)) throw new Error('Duplicate correlation ID')
        api = frame.readInt16BE(4)
        state.pending.set(id, api)
      } else {
        if (!state.pending.has(id)) throw new Error('Uncorrelated response')
        api = state.pending.get(id)
        state.pending.delete(id)
      }
      if (api === 1) {
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
 * @param {{connection:string,nodeId:number,direction:string,chunk:Buffer}[]} transcript
 * @returns {{bytes:number,requests:number,responses:number,frames:object[]}} frame evidence
 * @throws {Error} malformed, partial, duplicate, unmatched or unknown reply
 */
function auditTranscript(transcript) {
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
      if (request.apiKey === 1) frames.push({ ...frame, ...request })
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
module.exports = { FetchFrameCounter, auditTranscript, verifyFrames }
