const Connection = require('../../network/connection')
const FrameBuffer = require('../../network/frameBuffer')
const Encoder = require('../encoder')
const { KafkaJSConnectionClosedError, KafkaJSInvalidFrameError } = require('../../errors')
const { newLogger, createFakeSocketFactory, readRequestHeader } = require('testHelpers')
const { vectors, bodyOf, codecOf, project } = require('./fixtures/fuzzVectors')

/** A response is flexible, and has a tag buffer after its correlation id, from Fetch v12. */
const FIRST_FLEXIBLE_FETCH = 12

/** Lets every pending promise callback and socket event run. */
const settle = () => new Promise((done) => setImmediate(() => setImmediate(done)))

/**
 * The frame a broker sends for a vector: size, correlation id, the header's tag buffer when the
 * version has one, then the body.
 * @param {import('./fixtures/fuzzVectors').FuzzVector} vector
 * @param {number} correlationId
 * @returns {Buffer}
 */
const frameOf = (vector, correlationId = 0) => {
  const header = new Encoder().writeInt32(correlationId)
  if (vector.version >= FIRST_FLEXIBLE_FETCH) {
    header.writeTaggedFields()
  }
  const payload = Buffer.concat([header.buffer, bodyOf(vector)])
  const size = Buffer.alloc(4)
  size.writeInt32BE(payload.length)
  return Buffer.concat([size, payload])
}

/**
 * A connected `Connection` over a fake socket, and a way to send it Fetch requests whose
 * responses go through the vector's codec, counting each decode.
 * @param {import('./fixtures/fuzzVectors').FuzzVector} vector
 * @param {object} [options] more `Connection` options
 */
const connected = async (vector, options = {}) => {
  const { factory, state } = createFakeSocketFactory()
  const connection = new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    clientId: 'fuzz',
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
    ...options,
  })
  await connection.connect()

  const counters = { decoded: 0 }
  const codec = codecOf(vector)
  const response = {
    decode: (rawData) => {
      counters.decoded++
      return codec.decode(rawData, { checkCrcs: true })
    },
    parse: async (decoded) => decoded,
  }
  const request = {
    apiKey: 1,
    apiName: 'Fetch',
    apiVersion: vector.version,
    encode: async () => new Encoder(),
  }

  /** Sends a request; resolves with the correlation id it was written under, and its outcome. */
  const send = async () => {
    const outcome = connection.send({ request, response }).then(
      (value) => ({ value }),
      (error) => ({ error })
    )
    await settle()
    const { correlationId } = readRequestHeader(state.writes[state.writes.length - 1])
    return { correlationId, outcome }
  }

  return { connection, state, counters, send }
}

/** Whether `promise` has settled, without waiting for it. */
const hasSettled = async (promise) => {
  let settled = false
  promise.then(() => {
    settled = true
  })
  await settle()
  return settled
}

const SMALL = vectors.find(({ name }) => name === 'fetch-v18-java-null-records')
const WITH_RECORDS = vectors.find(({ name }) => name === 'fetch-v4-scala-six-batches')

describe('Protocol > Fetch response > a frame that is not finished', () => {
  describe('on a connection', () => {
    test.each([
      ['inside the size prefix', 3],
      ['after the size prefix', 4],
      ['inside the correlation id', 6],
      ['after the correlation id', 8],
      ['one byte short of the body', frameOf(SMALL).length - 1],
    ])(
      'a frame cut %s settles nothing, and a close settles the queue with the connection error',
      async (_, bytes) => {
        const { state, counters, send } = await connected(SMALL)
        const { correlationId, outcome } = await send()

        state.socket.emit('data', frameOf(SMALL, correlationId).subarray(0, bytes))

        expect(await hasSettled(outcome)).toBe(false)
        expect(counters.decoded).toBe(0)
        state.socket.emit('end')
        const { error } = await outcome
        expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
      }
    )

    test('a frame arriving one byte at a time decodes to the values of the vector, once', async () => {
      const { state, counters, send } = await connected(WITH_RECORDS)
      const { correlationId, outcome } = await send()
      const frame = frameOf(WITH_RECORDS, correlationId)

      for (let at = 0; at < frame.length; at++) {
        state.socket.emit('data', frame.subarray(at, at + 1))
        if (at < frame.length - 1) {
          expect(counters.decoded).toBe(0)
        }
      }

      const { value, error } = await outcome
      expect(error).toBeUndefined()
      expect(JSON.parse(JSON.stringify(project(value)))).toEqual(WITH_RECORDS.control)
      expect(counters.decoded).toBe(1)
    })

    test('two whole frames and the start of a third settle two requests, in order, and not the third', async () => {
      const { state, counters, send } = await connected(SMALL)
      const sent = [await send(), await send(), await send()]
      const [first, second, third] = sent.map(({ correlationId }) => frameOf(SMALL, correlationId))

      state.socket.emit('data', Buffer.concat([first, second, third.subarray(0, 10)]))
      await settle()

      expect(counters.decoded).toBe(2)
      expect(await hasSettled(sent[0].outcome)).toBe(true)
      expect(await hasSettled(sent[1].outcome)).toBe(true)
      expect(await hasSettled(sent[2].outcome)).toBe(false)
      state.socket.emit('data', third.subarray(10))
      expect((await sent[2].outcome).error).toBeUndefined()
      expect(counters.decoded).toBe(3)
    })

    test('a second response for a correlation id that was answered is ignored, and the connection goes on', async () => {
      const { state, counters, send } = await connected(SMALL)
      const first = await send()

      state.socket.emit('data', frameOf(SMALL, first.correlationId))
      expect((await first.outcome).error).toBeUndefined()
      state.socket.emit('data', frameOf(SMALL, first.correlationId))
      await settle()

      expect(counters.decoded).toBe(1)
      const second = await send()
      state.socket.emit('data', frameOf(SMALL, second.correlationId))
      expect((await second.outcome).error).toBeUndefined()
      expect(counters.decoded).toBe(2)
    })
  })

  describe('in the frame buffer', () => {
    /**
     * The storage the buffer holds, in the quantities of its equations: the capacity of the one
     * accumulator, the bytes received into it and the bytes copied in all.
     * @param {FrameBuffer} frameBuffer
     * @returns {{ capacity: number, received: number, copied: number }}
     */
    const countersOf = (frameBuffer) => frameBuffer.usage

    const INITIAL_CAPACITY = 64 * 1024

    const buffered = (vector, { maxBodySize = 100 * 1024 * 1024 } = {}) => {
      const frames = []
      const frameBuffer = new FrameBuffer({
        maxBodySize,
        onFrame: (body) => frames.push(Buffer.from(body)),
        minBodyOf: () => (vector.version >= FIRST_FLEXIBLE_FETCH ? 5 : 4),
      })
      return { frames, frameBuffer }
    }

    describe.each(vectors.map((vector) => [vector.name, vector]))('%s', (_, vector) => {
      test('cut at every byte, in one chunk or two, delivers nothing until the last byte and keeps its storage within bounds', () => {
        const frame = frameOf(vector)
        const body = frame.subarray(4)

        for (let bytes = 1; bytes < frame.length; bytes++) {
          const shapes = [[frame.subarray(0, bytes)]]
          if (bytes > 1) shapes.push([frame.subarray(0, 1), frame.subarray(1, bytes)])

          for (const chunks of shapes) {
            const { frames, frameBuffer } = buffered(vector)

            chunks.forEach((chunk) => frameBuffer.push(chunk))

            const { capacity, received } = countersOf(frameBuffer)
            expect({ bytes, frames: frames.length }).toEqual({ bytes, frames: 0 })
            expect(capacity).toBeLessThanOrEqual(body.length)
            expect(capacity).toBeLessThanOrEqual(Math.max(INITIAL_CAPACITY, 2 * received))
          }
        }

        const { frames, frameBuffer } = buffered(vector)
        frameBuffer.push(frame.subarray(0, 7))
        frameBuffer.push(frame.subarray(7))

        expect(frames).toEqual([body])
        expect(countersOf(frameBuffer).copied).toBeLessThan(3 * body.length)
      })
    })

    test('a prefix that already announces more than the cap is refused at once, with nothing allocated for it', () => {
      const { frames, frameBuffer } = buffered(SMALL, { maxBodySize: 1024 })
      const announcing = Buffer.alloc(4)
      announcing.writeInt32BE(4096)

      expect(() => frameBuffer.push(announcing)).toThrow(KafkaJSInvalidFrameError)
      expect(frames).toEqual([])
      expect(countersOf(frameBuffer).capacity).toBe(0)
    })

    test('a connection refuses it too: the request in flight fails with the frame error', async () => {
      const { state, send } = await connected(SMALL, { maxResponseSize: 1024 })
      const { outcome } = await send()
      const announcing = Buffer.alloc(4)
      announcing.writeInt32BE(4096)

      state.socket.emit('data', announcing)

      expect(await hasSettled(outcome)).toBe(true)
      const { error } = await outcome
      expect(error).toBeInstanceOf(KafkaJSInvalidFrameError)
      expect(error.reason).toBe('too-large')
    })
  })
})
