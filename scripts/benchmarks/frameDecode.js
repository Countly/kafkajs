/**
 * Response-frame decode benchmark: a `Connection` over a fake socket with requests in flight, and
 * their response frames delivered through the socket's `data` event, so the interval times the
 * frame loop (framing, matching a frame to its request, settling it) and nothing else: the codec
 * the requests carry only measures the body.
 * Usage: node --expose-gc scripts/benchmarks/frameDecode.js [--shape 64b|64k|1m|coalesced|byte-chunks]
 *          [--control 0|1]
 * Prints one JSON line of numbers: { frames, nsPerFrame, heapRetainedBytes } (the heap only under
 * `--expose-gc`).
 *
 * Declared before any run (session.js, medians of seven, the first run of each arm discarded, an
 * A/A arm, a quiet host): per shape the median ns per frame of this tree is within +3 % of the
 * previous tree's on every shape but `byte-chunks`, which is reported; the retained heap after
 * `gc()` is within +1 MiB of it. The positive control is `--control 1`, untimed: it counts the
 * calls of `RequestQueue#fulfillRequest`, which exists in both trees, and passes when they equal
 * the frames fed. A timed run carries no counter, and the script is the same bytes on both trees.
 *
 * Shapes: `64b` 64-byte bodies, a frame to a `data` event; `64k` and `1m` bodies of 64 KiB and
 * 1 MiB in 16 KiB chunks; `coalesced` 64 frames of 64 bytes in one chunk; `byte-chunks` one 64 KiB
 * frame a byte at a time.
 */
const { EventEmitter } = require('events')
const Connection = require('../../src/network/connection')
const RequestQueue = require('../../src/network/requestQueue')
const Encoder = require('../../src/protocol/encoder')
const { createLogger, LEVELS } = require('../../src/loggers')
const LoggerConsole = require('../../src/loggers/console')

const ROUNDS = 7
const SIZE_PREFIX = 4
const CORRELATION_ID_AT = 8

/**
 * @typedef {object} Shape
 * @property {number} body bytes of each response body
 * @property {number} frames frames a round feeds
 * @property {number} [chunk] bytes a `data` event carries of one frame, the whole frame by default
 * @property {number} [perChunk] frames in one `data` event
 */

/** @type {Record<string, Shape>} */
const SHAPES = {
  '64b': { body: 64, frames: 2000 },
  '64k': { body: 65536, frames: 200, chunk: 16384 },
  '1m': { body: 1048576, frames: 20, chunk: 16384 },
  coalesced: { body: 64, frames: 2048, perChunk: 64 },
  'byte-chunks': { body: 65536, frames: 2, chunk: 1 },
}

const USAGE = `usage: node scripts/benchmarks/frameDecode.js [--shape ${Object.keys(SHAPES).join('|')}] [--control 0|1]`

/**
 * @param {string[]} argv
 * @returns {{ shape: string, control: boolean }}
 * @throws {Error} the usage line for any argument it does not know
 */
const parseArgs = (argv) => {
  const options = { shape: '64b', control: false }
  for (let i = 0; i < argv.length; i += 2) {
    const [flag, value] = [argv[i], argv[i + 1]]
    if (flag === '--shape' && SHAPES[value]) {
      options.shape = value
    } else if (flag === '--control' && (value === '0' || value === '1')) {
      options.control = value === '1'
    } else {
      throw new Error(USAGE)
    }
  }

  return options
}

/** @returns {number} the median of the values: the upper middle of an even count */
const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * A socket that records what is written to it and lets the benchmark push bytes back.
 * @returns {{ factory: Function, writes: Buffer[], socket: () => EventEmitter }}
 */
const createFakeSocket = () => {
  const writes = []
  let current
  const factory = ({ onConnect }) => {
    current = new EventEmitter()
    current.write = (buffer) => {
      writes.push(Buffer.from(buffer))
      return true
    }
    current.end = () => {}
    current.unref = () => {}
    setImmediate(onConnect)
    return current
  }

  return { factory, writes, socket: () => current }
}

/**
 * The response frames of a shape for the requests written so far, cut into the chunks the shape
 * delivers.
 * @param {Shape} shape
 * @param {number[]} correlationIds one per frame
 * @returns {Buffer[]} the `data` events, in order
 */
const chunksOf = ({ body, chunk, perChunk = 1 }, correlationIds) => {
  const frames = correlationIds.map((correlationId) => {
    const payload = Buffer.alloc(body)
    const frame = new Encoder().writeInt32(SIZE_PREFIX + body).writeInt32(correlationId)
    return Buffer.concat([frame.buffer, payload])
  })

  if (chunk) {
    return frames.flatMap((frame) =>
      Array.from({ length: Math.ceil(frame.length / chunk) }, (_, at) =>
        frame.subarray(at * chunk, (at + 1) * chunk)
      )
    )
  }

  return Array.from({ length: Math.ceil(frames.length / perChunk) }, (_, at) =>
    Buffer.concat(frames.slice(at * perChunk, (at + 1) * perChunk))
  )
}

/**
 * One round: sends the requests, then times the delivery of every frame and the settling of every
 * request.
 * @param {Shape} shape
 * @param {number} frames
 * @returns {Promise<number>} nanoseconds per frame
 */
const round = async (shape, frames) => {
  const { factory, writes, socket } = createFakeSocket()
  const connection = new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: createLogger({ level: LEVELS.NOTHING, logCreator: LoggerConsole }),
    socketFactory: factory,
    clientId: 'bench',
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
    maxInFlightRequests: null,
  })
  await connection.connect()

  const request = { apiKey: 1, apiName: 'Fetch', apiVersion: 4, encode: async () => new Encoder() }
  const response = { decode: async (raw) => raw.length, parse: async (decoded) => decoded }
  const outcomes = Array.from({ length: frames }, () => connection.send({ request, response }))
  await new Promise((resolve) => setImmediate(resolve))
  const correlationIds = writes.map((write) => write.readInt32BE(CORRELATION_ID_AT))
  const chunks = chunksOf(shape, correlationIds)

  const startedAt = process.hrtime.bigint()
  for (const chunk of chunks) {
    socket().emit('data', chunk)
  }
  await Promise.all(outcomes)
  const elapsed = Number(process.hrtime.bigint() - startedAt)

  await connection.disconnect()
  return elapsed / frames
}

/**
 * @returns {Promise<number|undefined>} the heap in use after two collections, when node has `gc`
 */
const retainedHeap = async () => {
  if (typeof globalThis.gc !== 'function') {
    return undefined
  }

  globalThis.gc()
  await new Promise((resolve) => setTimeout(resolve, 100))
  globalThis.gc()
  return process.memoryUsage().heapUsed
}

/**
 * The timed run: an untimed round, then `ROUNDS` timed ones.
 * @param {string} name a key of `SHAPES`
 * @param {number} [frames] frames per round, the shape's by default
 * @returns {Promise<{ frames: number, nsPerFrame: number, heapRetainedBytes?: number }>}
 */
const timed = async (name, frames = SHAPES[name].frames) => {
  await round(SHAPES[name], frames)
  const rounds = []
  for (let i = 0; i < ROUNDS; i++) {
    rounds.push(await round(SHAPES[name], frames))
  }

  const heapRetainedBytes = await retainedHeap()
  return { frames, nsPerFrame: median(rounds), ...(heapRetainedBytes && { heapRetainedBytes }) }
}

/**
 * The control: one round with the request queue's `fulfillRequest` counted.
 * @param {string} name a key of `SHAPES`
 * @param {number} [frames]
 * @returns {Promise<{ control: true, shape: string, framesFed: number, fulfilled: number,
 *   pass: boolean }>} `pass` when the counted calls equal the frames fed
 */
const control = async (name, frames = SHAPES[name].frames) => {
  const original = RequestQueue.prototype.fulfillRequest
  let fulfilled = 0
  RequestQueue.prototype.fulfillRequest = function fulfillRequest(...args) {
    fulfilled++
    return original.apply(this, args)
  }

  try {
    await round(SHAPES[name], frames)
  } finally {
    RequestQueue.prototype.fulfillRequest = original
  }

  return { control: true, shape: name, framesFed: frames, fulfilled, pass: fulfilled === frames }
}

if (require.main === module) {
  ;(async () => {
    const { shape, control: controlRun } = parseArgs(process.argv.slice(2))
    const result = controlRun ? await control(shape) : await timed(shape)
    console.log(JSON.stringify(result))
    process.exitCode = result.pass === false ? 1 : 0
  })().catch((e) => {
    console.error(e)
    process.exit(1)
  })
}

module.exports = { SHAPES, parseArgs, chunksOf, timed, control }
