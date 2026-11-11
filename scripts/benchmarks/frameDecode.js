/**
 * Response-frame decode benchmark: a `Connection` over a fake socket with requests in flight, and
 * their response frames delivered through the socket's `data` event, so the interval times the
 * frame loop (framing, matching a frame to its request, settling it) and nothing else: the codec
 * the requests carry only measures the body.
 * Usage: node --expose-gc scripts/benchmarks/frameDecode.js [--shape 64b|64k|1m|coalesced|byte-chunks]
 *          [--control 0|1] [--work-multiplier <k>]
 * Prints one JSON line: { frames, workMultiplier, nsPerFrame, warmupRoundNsPerOp, roundNsPerOp, heapRetainedBytes } (the heap only under
 * `--expose-gc`).
 *
 * Timed mode requires `--expose-gc`: three discarded warm-up rounds, then seven measured
 * rounds, collecting after setup before each round's first interval. Sequential repetitions
 * exclude setup and disconnect from elapsed time. Only measured rounds set the median.
 * The positive control is `--control 1`, untimed: it counts `RequestQueue#fulfillRequest` calls
 * and passes when they equal the frames fed. It does not require GC. The comparison overlays this
 * script unchanged into both trees and judges paired bounds with a calibrated baseline control.
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

const WARMUP_ROUNDS = 3
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

const USAGE = `usage: node scripts/benchmarks/frameDecode.js [--shape ${Object.keys(SHAPES).join('|')}] [--control 0|1] [--work-multiplier <k>]`

/**
 * @param {string[]} argv
 * @returns {{ shape: string, control: boolean, workMultiplier?: number }}
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
    } else if (
      flag === '--work-multiplier' &&
      Number.isSafeInteger(Number(value)) &&
      Number(value) > 0
    ) {
      options.workMultiplier = Number(value)
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
    current.destroyed = false
    current.destroy = () => {
      current.destroyed = true
    }
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
 * One fixture: sends requests, then times frame delivery and request settlement. Repetitions
 * keep setup and disconnect outside timing and never increase concurrent requests.
 * @param {Shape} shape
 * @param {number} frames
 * @param {boolean} [collect] collect after setup, before the timed interval
 * @returns {Promise<number>} nanoseconds per frame
 */
const round = async (shape, frames, collect = false) => {
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

  if (collect) globalThis.gc()
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
 * Requires exposed GC. Three discarded warmups, then seven measured rounds; GC after setup
 * precedes each round's first interval. Each round averages repeated fixture timings; warmup
 * timings remain evidence but never enter the median.
 * @param {string} name a key of `SHAPES`
 * @param {number} [frames] frames per fixture, the shape's by default
 * @param {number} [workMultiplier] sequential fixtures per round; GC precedes the first interval
 * @throws {Error} when globalThis.gc is unavailable or the multiplier is not a positive integer
 * @returns {Promise<{ frames: number, workMultiplier: number, nsPerFrame: number, roundNsPerOp: number[], warmupRoundNsPerOp: number[], heapRetainedBytes?: number }>}
 */
const timed = async (name, frames = SHAPES[name].frames, workMultiplier = 1) => {
  if (typeof globalThis.gc !== 'function') throw new Error('timed mode requires --expose-gc')
  if (!Number.isSafeInteger(workMultiplier) || workMultiplier < 1) throw new Error(USAGE)
  const repeatedRound = async () => {
    let nsPerFrame = 0
    for (let repetition = 0; repetition < workMultiplier; repetition++) {
      nsPerFrame += await round(SHAPES[name], frames, repetition === 0)
    }
    return nsPerFrame / workMultiplier
  }
  const warmupRoundNsPerOp = []
  for (let i = 0; i < WARMUP_ROUNDS; i++) {
    warmupRoundNsPerOp.push(await repeatedRound())
  }
  const rounds = []
  for (let i = 0; i < ROUNDS; i++) {
    rounds.push(await repeatedRound())
  }

  const heapRetainedBytes = await retainedHeap()
  return {
    frames,
    workMultiplier,
    nsPerFrame: median(rounds),
    roundNsPerOp: rounds,
    warmupRoundNsPerOp,
    ...(heapRetainedBytes && { heapRetainedBytes }),
  }
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
    const { shape, control: controlRun, workMultiplier } = parseArgs(process.argv.slice(2))
    const result = controlRun ? await control(shape) : await timed(shape, undefined, workMultiplier)
    console.log(JSON.stringify(result))
    process.exitCode = result.pass === false ? 1 : 0
  })().catch((e) => {
    console.error(e)
    process.exit(1)
  })
}

module.exports = { SHAPES, parseArgs, chunksOf, timed, control }
