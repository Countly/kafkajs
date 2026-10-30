const { FetchFrameCounter, auditTranscript, verifyFrames } = require('./fetchFrameCounter')

/**
 * Owns raw socket evidence and an independent primary Fetch byte counter.
 * Observations include every API so response correlation excludes unrelated replies.
 * @class
 */
class FetchObservation {
  /**
   * @returns {FetchObservation} empty per-run observer
   * @throws Never.
   */
  constructor() {
    this.counter = new FetchFrameCounter()
    this.transcript = []
    this.serial = 0
  }

  /**
   * Records the unmodified chunk before invoking the primary counter.
   * @param {string} connection nonreused socket identity
   * @param {number} nodeId broker identity, independent of connection identity
   * @param {'request'|'response'} direction observation direction
   * @param {Buffer} chunk actual socket bytes
   * @returns {void} independent evidence and primary counters
   * @throws {Error} malformed primary frame
   */
  push(connection, nodeId, direction, chunk) {
    this.transcript.push({
      connection,
      nodeId,
      direction,
      chunk: Buffer.from(chunk),
    })
    this.counter.push(connection, direction, chunk)
  }

  /**
   * Wraps a public socketFactory without changing write callbacks or backpressure.
   * Attach before the client registers its response listener. A reconnect gets a new key.
   * @param {Function} base existing public socketFactory
   * @param {Function} nodeFor maps host/port to broker ID
   * @returns {Function} factory for Kafka constructor's socketFactory option
   * @throws {Error} underlying factory or invalid observation
   */
  socketFactory(base, nodeFor) {
    return (options) => {
      const socket = base(options)
      const connection = String(++this.serial)
      const nodeId = nodeFor(options)
      const write = socket.write
      socket.write = (...args) => {
        this.push(
          connection,
          nodeId,
          'request',
          Buffer.from(args[0], typeof args[1] === 'string' ? args[1] : undefined)
        )
        return write.apply(socket, args)
      }
      const data = (chunk) => this.push(connection, nodeId, 'response', chunk)
      socket.prependListener('data', data)
      socket.once('close', () => {
        socket.removeListener('data', data)
        socket.write = write
      })
      return socket
    }
  }

  /**
   * Checks the complete raw transcript independently against primary counters.
   * @returns {object} counted frames and transcript totals
   * @throws {Error} partial window or mismatched counters
   */
  result() {
    const audit = auditTranscript(this.transcript)
    verifyFrames(this.counter.result(), audit)
    return audit
  }
}

/**
 * Verifies a declared broker cycle using independently counted request frames.
 * This readiness control checks denominators; session-body correctness belongs later.
 * @param {object[]} frames independent counted transcript
 * @param {{full:boolean,nodes?:number[]}} options cycle mode and broker IDs
 * @returns {number} total request bytes across the whole broker cycle
 * @throws {Error} wrong broker cycle, full size or idle incremental request size
 */
function verifyCycle(frames, { full, nodes = [0, 1, 2] }) {
  const requests = frames.filter((frame) => frame.direction === 'request')
  if (
    requests.length !== nodes.length ||
    nodes.some((node) => requests.filter((frame) => frame.nodeId === node).length !== 1)
  )
    throw new Error('Incomplete broker cycle')
  const bytes = requests.reduce((total, frame) => total + frame.bytes, 0)
  if (full && bytes < 20000) throw new Error('Full cycle below 20000 request bytes')
  if (!full && requests.some((frame) => frame.bytes > 128))
    throw new Error('Idle incremental request above 128 bytes')
  return bytes
}

/**
 * Runs finite, offline frame-denominator controls, using independent header literals.
 * @returns {object} fixed 334/333/333 partition shape and observed full-cycle bytes
 * @throws {Error} broken frame accounting or cycle denominator
 */
function control() {
  const observation = new FetchObservation()
  for (const [nodeId, partitions] of [334, 333, 333].entries()) {
    const request = Buffer.alloc(14 + partitions * 24)
    request.writeInt32BE(request.length - 4)
    request.writeInt16BE(1, 4)
    request.writeInt16BE(12, 6)
    request.writeInt32BE(7, 8)
    request.writeInt16BE(0, 12)
    const response = Buffer.alloc(8)
    response.writeInt32BE(4)
    response.writeInt32BE(7, 4)
    observation.push(String(nodeId), nodeId, 'request', request)
    observation.push(String(nodeId), nodeId, 'response', response)
  }
  const result = observation.result()
  return {
    shape: [334, 333, 333],
    requestBytes: verifyCycle(result.frames, { full: true }),
    ...result,
  }
}

/**
 * Provides the public-driver readiness entry. Timed live arms are added by the final driver.
 * @param {string[]} [args] only --control at this milestone
 * @returns {void} one offline control JSON record
 * @throws {Error} unsupported timed use or incorrect control
 */
function main(args = process.argv.slice(2)) {
  if (args.length !== 1 || args[0] !== '--control')
    throw new Error('Only --control is available in this driver')
  process.stdout.write(JSON.stringify(control()) + '\n')
}
if (require.main === module) {
  try {
    main()
  } catch (error) {
    process.stderr.write(error.stack + '\n')
    process.exitCode = 1
  }
}
module.exports = { FetchObservation, verifyCycle, control, main }
