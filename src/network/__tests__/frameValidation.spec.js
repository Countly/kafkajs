const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const Connection = require('../connection')
const FrameBuffer = require('../frameBuffer')
const InstrumentationEmitter = require('../../instrumentation/emitter')
const { NETWORK_REQUEST, NETWORK_REQUEST_QUEUE_SIZE } = require('../instrumentationEvents')
const { Kafka, logLevel } = require('../../../index')
const { KafkaJSConnectionError, KafkaJSInvalidFrameError } = require('../../errors')
const { newLogger, connectionOpts } = require('testHelpers')
const { createFakeSocketFactory, respondWithFrame } = require('../../../testHelpers/fakeSocket')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')

const KIB = 1024
const MIB = 1024 * KIB

/** One response frame: the size prefix, the correlation id, then `payload`. */
const frameOf = (correlationId, payload = Buffer.alloc(0)) => {
  const body = Buffer.concat([Buffer.alloc(4), payload])
  body.writeInt32BE(correlationId, 0)
  const frame = Buffer.alloc(4 + body.length)
  frame.writeInt32BE(body.length, 0)
  body.copy(frame, 4)
  return frame
}

/** The four size bytes alone. */
const prefixOf = (size) => {
  const prefix = Buffer.alloc(4)
  prefix.writeInt32BE(size, 0)
  return prefix
}

/** A buffer that records the frames it delivers, with a copy of each. */
const recording = (options = {}) => {
  const frames = []
  const buffer = new FrameBuffer({
    maxBodySize: 100 * MIB,
    onFrame: (body) => frames.push(Buffer.from(body)),
    ...options,
  })

  return { buffer, frames }
}

/** Waits for a promise, failing with `what` after `ms` instead of leaving the test to time out. */
const within = (promise, ms, what) => {
  let timer
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} did not happen within ${ms} ms`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

const invalid = (action) => {
  try {
    action()
  } catch (error) {
    return error
  }

  throw new Error('expected an invalid frame')
}

describe('FrameBuffer > validation before the body is waited for', () => {
  it.each([
    ['negative', -1, 'negative'],
    ['-5', -5, 'negative'],
    ['the smallest int32', -2147483648, 'negative'],
    ['too short for a correlation id', 3, 'too-short'],
    ['empty', 0, 'too-short'],
  ])('refuses a frame of size %s', (_name, size, reason) => {
    const { buffer, frames } = recording()

    const error = invalid(() => buffer.push(prefixOf(size)))

    expect(error).toBeInstanceOf(KafkaJSInvalidFrameError)
    expect(error).toMatchObject({ reason, size, retriable: true })
    expect(error.message).toContain(`${size}`)
    expect(frames).toEqual([])
  })

  it('refuses a frame above the limit, naming the option, the size and the limit', () => {
    const { buffer } = recording({ maxBodySize: 1000, broker: 'kafka1:9092' })

    const error = invalid(() => buffer.push(prefixOf(2 * MIB)))

    expect(error).toMatchObject({ reason: 'too-large', size: 2 * MIB, broker: 'kafka1:9092' })
    expect(error.message).toContain('maxResponseSize (1000)')
    expect(error.message).toContain(`${2 * MIB}`)
  })

  it('refuses 2 GiB without allocating anything for it', () => {
    const { buffer } = recording()

    const error = invalid(() => buffer.push(prefixOf(2147483647)))

    expect(error.reason).toBe('too-large')
    expect(buffer.usage).toEqual({ received: 0, capacity: 0, copied: 0 })
  })

  it('names an HTTP answer and a TLS alert in the message', () => {
    const http = invalid(() => recording().buffer.push(Buffer.from('HTTP/1.1 400 Bad Request')))
    const tls = invalid(() => recording().buffer.push(Buffer.from([21, 3, 3, 0, 2, 2, 40])))

    expect(http.message).toContain('"HTTP"')
    expect(http.message).toContain('broker port')
    expect(tls.message).toContain('TLS alert')
    expect(tls.message).toContain('plaintext connection')
    expect(tls.reason).toBe('too-large')
  })

  it('accepts exactly the limit and refuses one byte above it', () => {
    const { buffer, frames } = recording({ maxBodySize: 64 })

    buffer.push(frameOf(1, Buffer.alloc(60)))
    expect(frames).toHaveLength(1)
    expect(frames[0]).toHaveLength(64)
    expect(invalid(() => buffer.push(prefixOf(65))).reason).toBe('too-large')
  })

  it('accepts a body of exactly a correlation id', () => {
    const { buffer, frames } = recording()

    buffer.push(frameOf(7))

    expect(frames).toEqual([prefixOf(7)])
  })

  it('refuses a body too short for a flexible response header, naming the request it answers', () => {
    const minBodyOf = jest.fn(() => 5)
    const { buffer, frames } = recording({ minBodyOf })

    const error = invalid(() => buffer.push(frameOf(42)))

    expect(minBodyOf).toHaveBeenCalledWith(42)
    expect(error).toMatchObject({ reason: 'too-short', size: 4 })
    expect(error.message).toContain('correlation id 42')
    expect(frames).toEqual([])
  })

  it('delivers a valid frame for a correlation id nobody is waiting for', () => {
    const { buffer, frames } = recording({ minBodyOf: () => 4 })

    buffer.push(frameOf(999, Buffer.from('late')))

    expect(frames).toHaveLength(1)
  })

  it('is told the minimum once per frame, however the frame is split', () => {
    const minBodyOf = jest.fn(() => 4)
    const { buffer } = recording({ minBodyOf })

    for (const byte of frameOf(5, Buffer.from('abcdef'))) {
      buffer.push(Buffer.from([byte]))
    }

    expect(minBodyOf).toHaveBeenCalledTimes(1)
    expect(minBodyOf).toHaveBeenCalledWith(5)
  })
})

describe('FrameBuffer > splitting and delivery', () => {
  const frame = frameOf(0x01020304, Buffer.from('payload'))

  it.each([
    ['1+3', 1],
    ['2+2', 2],
    ['3+1', 3],
  ])('reads a size prefix split %s across chunks', (_name, at) => {
    const { buffer, frames } = recording()

    buffer.push(frame.subarray(0, at))
    expect(frames).toEqual([])
    buffer.push(frame.subarray(at))

    expect(frames).toEqual([frame.subarray(4)])
  })

  it('reads a correlation id split across chunks', () => {
    const { buffer, frames } = recording()

    buffer.push(frame.subarray(0, 6))
    buffer.push(frame.subarray(6, 9))
    buffer.push(frame.subarray(9))

    expect(frames).toEqual([frame.subarray(4)])
  })

  it('delivers one frame arriving a byte at a time', () => {
    const { buffer, frames } = recording()

    for (const byte of frame) {
      buffer.push(Buffer.from([byte]))
    }

    expect(frames).toEqual([frame.subarray(4)])
  })

  it('delivers two and three frames coalesced in one chunk, in order', () => {
    const { buffer, frames } = recording()
    const a = frameOf(1, Buffer.from('a'))
    const b = frameOf(2, Buffer.from('bb'))
    const c = frameOf(3, Buffer.from('ccc'))

    buffer.push(Buffer.concat([a, b]))
    buffer.push(Buffer.concat([a, b, c]))

    expect(frames).toEqual([a, b, a, b, c].map((each) => each.subarray(4)))
  })

  it('delivers the valid frames before a bad one, and nothing after it', () => {
    const { buffer, frames } = recording({ maxBodySize: 1000 })
    const good = frameOf(1, Buffer.from('ok'))

    const error = invalid(() =>
      buffer.push(Buffer.concat([good, prefixOf(5000), Buffer.alloc(8), frameOf(3)]))
    )

    expect(error.reason).toBe('too-large')
    expect(frames).toEqual([good.subarray(4)])
  })

  it('delivers a frame that completes in the same chunk as the next frame begins', () => {
    const { buffer, frames } = recording()
    const a = frameOf(1, Buffer.from('aaaa'))
    const b = frameOf(2, Buffer.from('bbbb'))

    buffer.push(a.subarray(0, 7))
    buffer.push(Buffer.concat([a.subarray(7), b.subarray(0, 5)]))
    buffer.push(b.subarray(5))

    expect(frames).toEqual([a.subarray(4), b.subarray(4)])
  })

  it('clears the buffer and rethrows when the callback throws, then starts clean', () => {
    const delivered = []
    const buffer = new FrameBuffer({
      maxBodySize: MIB,
      onFrame: (body) => {
        if (body.length === 6) {
          throw new Error('callback failed')
        }
        delivered.push(Buffer.from(body))
      },
    })

    const frame = frameOf(1, Buffer.from('ab'))

    buffer.push(frame.subarray(0, 7))
    expect(() => buffer.push(frame.subarray(7))).toThrow('callback failed')
    expect(buffer.usage).toEqual({ received: 0, capacity: 0, copied: 0 })
    buffer.push(frameOf(2, Buffer.from('abc')))

    expect(delivered).toHaveLength(1)
  })

  it('drops a half-read size prefix on reset', () => {
    const { buffer, frames } = recording()

    buffer.push(Buffer.from([0, 0]))
    buffer.reset()
    buffer.push(frameOf(3, Buffer.from('fresh')))

    expect(frames).toEqual([frameOf(3, Buffer.from('fresh')).subarray(4)])
  })

  it('drops the bytes of a frame in progress on reset, so a new connection starts clean', () => {
    const { buffer, frames } = recording()

    buffer.push(frameOf(1, Buffer.alloc(100)).subarray(0, 20))
    buffer.reset()
    buffer.push(frameOf(2, Buffer.from('fresh')))

    expect(frames).toEqual([frameOf(2, Buffer.from('fresh')).subarray(4)])
    expect(buffer.usage.copied).toBe(0)
  })
})

describe('FrameBuffer > retained memory and copying', () => {
  const INITIAL = 64 * KIB

  /** Feeds `frame` in chunks of `chunkSize`, checking the bounds after every chunk. */
  const feed = (buffer, frame, chunkSize) => {
    const size = frame.length - 4
    for (let offset = 0; offset < frame.length; offset += chunkSize) {
      buffer.push(frame.subarray(offset, offset + chunkSize))
      const { received, capacity } = buffer.usage
      expect(capacity).toBeLessThanOrEqual(size)
      expect(capacity).toBeLessThanOrEqual(Math.max(INITIAL, 2 * received))
    }
  }

  it('delivers a frame that arrives whole as a view of the chunk, with no copy', () => {
    const { buffer } = recording()
    const chunk = frameOf(1, Buffer.alloc(100 * KIB, 7))
    let body

    buffer.onFrame = (delivered) => {
      body = delivered
    }
    buffer.push(chunk)

    expect(body.buffer).toBe(chunk.buffer)
    expect(body.byteOffset).toBe(chunk.byteOffset + 4)
    expect(buffer.usage.copied).toBe(0)
  })

  it('holds the first allocation for a peer that announces 100 MiB and sends one byte', () => {
    const { buffer } = recording()

    buffer.push(prefixOf(100 * MIB))
    buffer.push(Buffer.from([0]))

    expect(buffer.usage).toEqual({ received: 1, capacity: INITIAL, copied: 1 })
  })

  it('holds no more than 64 KiB for a small frame with a few bytes received', () => {
    const { buffer } = recording()

    buffer.push(prefixOf(INITIAL))
    buffer.push(Buffer.alloc(10))

    expect(buffer.usage.capacity).toBe(INITIAL)
  })

  it('stays within the equations for a 3 MiB + 17 byte frame in 16 KiB chunks', () => {
    const { buffer, frames } = recording()
    const size = 3 * MIB + 17
    const payload = Buffer.alloc(size - 4, 9)

    feed(buffer, frameOf(1, payload), 16 * KIB)

    expect(frames).toHaveLength(1)
    expect(frames[0].subarray(4).equals(payload)).toBe(true)
    expect(buffer.usage.copied).toBeLessThan(3 * size)
  })

  it('copies fewer than three times the frame size, one byte at a time, at the limit', () => {
    const size = MIB
    const { buffer, frames } = recording({ maxBodySize: size })
    let copied = 0

    buffer.onFrame = jest.fn(() => {
      ;({ copied } = buffer.usage)
    })
    // The counter resets with the frame, so read it just before the last byte completes it.
    const frame = frameOf(1, Buffer.alloc(size - 4))
    for (let offset = 0; offset < frame.length - 1; offset++) {
      buffer.push(frame.subarray(offset, offset + 1))
    }
    copied = buffer.usage.copied
    buffer.push(frame.subarray(frame.length - 1))

    expect(buffer.onFrame).toHaveBeenCalledTimes(1)
    expect(copied).toBeLessThan(3 * size)
    expect(frames).toEqual([])
  })

  it('counts what a growth copies as well as what arrives: 3 chunks of 64 KiB cost twice the frame', () => {
    const { buffer, frames } = recording()
    const size = 3 * INITIAL

    buffer.push(prefixOf(size))
    for (let chunk = 0; chunk < 3; chunk++) {
      buffer.push(Buffer.alloc(INITIAL))
    }

    expect(frames).toHaveLength(1)
    expect(buffer.usage.copied).toBe(2 * size)
  })

  it('allocates a frame of at most 64 KiB once and copies each byte once', () => {
    const { buffer } = recording()
    const frame = frameOf(1, Buffer.alloc(10 * KIB))

    feed(buffer, frame, 1000)

    expect(buffer.usage.copied).toBe(frame.length - 4)
  })
})

describe('Connection > a hostile or corrupt response frame', () => {
  const protocol = {
    request: {
      apiKey: apiKeys.ApiVersions,
      apiVersion: 0,
      apiName: 'ApiVersions',
      encode: async () => new Encoder(),
    },
    response: { decode: async (payload) => payload, parse: async (decoded) => decoded },
  }

  let fake
  let connection

  const connected = async (options = {}) => {
    fake = createFakeSocketFactory()
    connection = new Connection({
      host: 'kafka1',
      port: 9092,
      logger: newLogger(),
      socketFactory: fake.factory,
      requestTimeout: 1000,
      connectionTimeout: 1000,
      ...options,
    })
    await connection.connect()
    return fake.state.socket
  }

  /** How a request ended, within a second: 'resolved', or the error it was rejected with. */
  const answered = (promise) =>
    within(
      promise.then(
        () => 'resolved',
        (error) => error
      ),
      1000,
      'the request to settle'
    )

  afterEach(async () => {
    await connection?.disconnect({ drainTimeout: 0 })
  })

  it.each([
    ['a negative size', prefixOf(-5), 'negative'],
    ['-1', prefixOf(-1), 'negative'],
    ['2 GiB', prefixOf(2147483647), 'too-large'],
    ['an HTTP answer', Buffer.from('HTTP/1.1 400 Bad Request\r\n'), 'too-large'],
    ['a TLS alert', Buffer.from([21, 3, 3, 0, 2, 2, 40]), 'too-large'],
    ['a frame too short for a correlation id', prefixOf(2), 'too-short'],
  ])(
    'closes the connection on %s, failing the request, and throws nothing into the listener',
    async (_name, bytes, reason) => {
      const socket = await connected()
      const pending = answered(connection.send(protocol))
      await new Promise(setImmediate)

      expect(() => socket.emit('data', bytes)).not.toThrow()

      const error = await pending
      expect(error).toBeInstanceOf(KafkaJSInvalidFrameError)
      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error).toMatchObject({ reason, retriable: true, broker: 'kafka1:9092' })
      expect(socket.destroyed).toBe(true)
      expect(connection.socket).toBeNull()
      expect(connection.isConnected()).toBe(false)
      expect(connection.frames.usage).toEqual({ received: 0, capacity: 0, copied: 0 })
    }
  )

  it('fails every request on the connection with the frame error, not only one', async () => {
    const socket = await connected({ maxInFlightRequests: 1 })
    const first = answered(connection.send(protocol))
    const second = answered(connection.send(protocol))
    await new Promise(setImmediate)

    socket.emit('data', prefixOf(-1))

    expect(await first).toBeInstanceOf(KafkaJSInvalidFrameError)
    expect(await second).toBeInstanceOf(KafkaJSInvalidFrameError)
    expect(fake.state.writes).toHaveLength(1)
  })

  it('delivers the valid frame before a bad one, in the same chunk, and nothing after it', async () => {
    const socket = await connected()
    const first = answered(connection.send(protocol))
    const second = answered(connection.send(protocol))
    const third = answered(connection.send(protocol))
    await new Promise(setImmediate)

    socket.emit('data', Buffer.concat([frameOf(0), prefixOf(-1), frameOf(2)]))

    expect(await first).toBe('resolved')
    expect(await second).toBeInstanceOf(KafkaJSInvalidFrameError)
    expect(await third).toBeInstanceOf(KafkaJSInvalidFrameError)
  })

  it('ignores the data of a socket whose generation ended', async () => {
    const socket = await connected()
    const pending = answered(connection.send(protocol))
    await new Promise(setImmediate)
    socket.emit('data', prefixOf(-1))
    await pending

    expect(() => socket.emit('data', prefixOf(-1))).not.toThrow()
    expect(socket.listenerCount('data')).toBe(0)
  })

  it('does not carry the partial frame of a dead socket into the next connection', async () => {
    const socket = await connected()
    const dead = answered(connection.send(protocol))
    await new Promise(setImmediate)
    socket.emit('data', Buffer.concat([prefixOf(1000), Buffer.alloc(2)]))
    socket.emit('end')
    expect(await dead).toBeInstanceOf(Error)
    await new Promise(setImmediate)

    await connection.connect()
    const next = connection.send(protocol)
    await new Promise(setImmediate)
    respondWithFrame({ socket: fake.state.socket }, 1, 0, Buffer.from('fresh'))

    expect(fake.state.sockets).toHaveLength(2)
    await expect(within(next, 1000, 'the response')).resolves.toEqual(Buffer.from('fresh'))
  })

  it('fails the connection with an unreadable-frame error when a delivered frame cannot be handled', async () => {
    const socket = await connected()
    const pending = answered(connection.send(protocol))
    await new Promise(setImmediate)
    jest.spyOn(connection.requestQueue, 'fulfillRequest').mockImplementation(() => {
      throw new Error('cannot fulfil')
    })

    expect(() => socket.emit('data', frameOf(0))).not.toThrow()

    const error = await pending
    expect(error).toMatchObject({ reason: 'unreadable', size: null })
    expect(error.cause.message).toBe('cannot fulfil')
    expect(socket.destroyed).toBe(true)
  })

  const THROWN = [
    ['an Error', new Error('observer failed')],
    ['null', null],
    ['undefined', undefined],
  ]

  it.each(
    [
      [
        'a queue-size listener',
        (emitter, cause) =>
          emitter.addListener(NETWORK_REQUEST_QUEUE_SIZE, () => {
            throw cause
          }),
      ],
      [
        'a completion listener',
        (emitter, cause) =>
          emitter.addListener(NETWORK_REQUEST, () => {
            throw cause
          }),
      ],
      [
        'the diagnostic of the pending request',
        (_emitter, cause) =>
          jest.spyOn(connection.requestQueue.logger, 'debug').mockImplementation((message) => {
            if (message === 'Consumed pending request') throw cause
          }),
      ],
    ].flatMap(([name, breakObserver]) =>
      THROWN.map(([what, cause]) => [name, what, breakObserver, cause])
    )
  )(
    'delivers the response being fulfilled, and fails the rest, when %s throws %s',
    async (_name, _what, breakObserver, cause) => {
      const emitter = new InstrumentationEmitter()
      const socket = await connected({ maxInFlightRequests: 1, instrumentationEmitter: emitter })
      const first = connection.send(protocol)
      const second = answered(connection.send(protocol))
      await new Promise(setImmediate)
      expect(connection.requestQueue.pending).toHaveLength(1)
      breakObserver(emitter, cause)

      expect(() => socket.emit('data', frameOf(0, Buffer.from('first')))).not.toThrow()

      await expect(within(first, 1000, 'the first response')).resolves.toEqual(Buffer.from('first'))
      const failure = await second
      expect(failure).toMatchObject({ name: 'KafkaJSInvalidFrameError', reason: 'unreadable' })
      expect(failure.cause).toBe(cause)
      expect(socket.destroyed).toBe(true)
      expect(connection.requestQueue.inflight.size).toBe(0)
      expect(connection.requestQueue.pending).toHaveLength(0)
    }
  )

  it('knows a flexible response header needs five bytes, and an ApiVersions one only four', async () => {
    const socket = await connected()
    const flexible = {
      ...protocol,
      request: {
        ...protocol.request,
        apiKey: apiKeys.Metadata,
        apiVersion: 13,
        apiName: 'Metadata',
      },
    }
    const pending = answered(connection.send(flexible))
    await new Promise(setImmediate)

    socket.emit('data', frameOf(0))

    expect(await pending).toMatchObject({ reason: 'too-short', size: 4 })
  })

  it('accepts a response of exactly the limit and refuses one byte above it', async () => {
    const limit = 32
    const socket = await connected({ maxResponseSize: limit })
    const ok = connection.send(protocol)
    await new Promise(setImmediate)
    socket.emit('data', frameOf(0, Buffer.alloc(limit - 4)))
    await expect(within(ok, 1000, 'the response')).resolves.toBeInstanceOf(Buffer)

    const tooBig = answered(connection.send(protocol))
    await new Promise(setImmediate)
    socket.emit('data', frameOf(1, Buffer.alloc(limit - 3)))

    expect(await tooBig).toMatchObject({ reason: 'too-large', size: limit + 1 })
  })

  it('delivers a valid frame for a correlation id nobody waits for, and stays connected', async () => {
    const socket = await connected()

    expect(() => socket.emit('data', frameOf(4242, Buffer.from('late')))).not.toThrow()

    expect(connection.isConnected()).toBe(true)
    expect(socket.destroyed).toBe(false)
  })

  it('rebuilds its framing state on every connect', async () => {
    const socket = await connected()
    socket.emit('data', Buffer.concat([prefixOf(1000), Buffer.alloc(2)]))
    expect(connection.frames.usage.received).toBe(2)
    await connection?.disconnect()
    expect(connection.frames.usage).toEqual({ received: 0, capacity: 0, copied: 0 })

    connection.frames.push(Buffer.concat([prefixOf(1000), Buffer.alloc(3)]))
    await connection.connect()

    expect(connection.frames.usage).toEqual({ received: 0, capacity: 0, copied: 0 })
  })

  it('hands the queue the correlation id, the size of the body and the rest as the payload', async () => {
    const socket = await connected()
    const fulfil = jest.spyOn(connection.requestQueue, 'fulfillRequest')
    const pending = connection.send(protocol)
    await new Promise(setImmediate)

    socket.emit('data', frameOf(0, Buffer.from('abc')))

    await within(pending, 1000, 'the response')
    expect(fulfil).toHaveBeenCalledWith({ correlationId: 0, size: 7, payload: Buffer.from('abc') })
  })
})

describe('maxResponseSize through the client', () => {
  const NODES = [{ id: 0, host: 'node0', port: 9092 }]

  it('fails fast and bounded, naming the option, when it is below a normal response', async () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    const admin = new Kafka(
      cluster.kafkaOptions({
        maxResponseSize: 8,
        retry: { retries: 2, initialRetryTime: 5, maxRetryTime: 10 },
      })
    ).admin()

    const error = await admin.connect().then(
      () => null,
      (e) => e
    )
    await admin.disconnect().catch(() => {})

    expect(error).toMatchObject({ name: 'KafkaJSNumberOfRetriesExceeded' })
    expect(error.cause).toBeInstanceOf(KafkaJSInvalidFrameError)
    expect(error.cause).toMatchObject({ reason: 'too-large', retriable: true })
    expect(error.cause.message).toContain('maxResponseSize (8)')
    expect(cluster.sockets().length).toBeGreaterThan(1)
    expect(cluster.sockets().every((socket) => socket.destroyed)).toBe(true)
    cluster.assertClean()
  })

  it('works with the default limit', async () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    const admin = new Kafka(cluster.kafkaOptions()).admin()

    await admin.connect()
    await admin.disconnect()

    cluster.assertClean()
  })
})

describe('Kafka#consumer and the response-size limit', () => {
  const logged = []
  const logCreator = () => (entry) => logged.push(entry)
  const kafka = (maxResponseSize) =>
    new Kafka({
      brokers: ['localhost:9092'],
      logCreator,
      maxResponseSize,
    })

  beforeEach(() => {
    logged.length = 0
  })

  it('warns once when maxBytes is above maxResponseSize', () => {
    kafka(1000).consumer({ groupId: 'g', maxBytes: 5000 })

    const warnings = logged.filter(({ level }) => level === logLevel.WARN)
    expect(warnings).toHaveLength(1)
    expect(warnings[0].log.message).toContain('maxBytes (5000) is above maxResponseSize (1000)')
    expect(warnings[0].log.message).toContain('KafkaJSInvalidFrameError')
  })

  it.each([
    ['maxBytes equal to the limit', 1000, { maxBytes: 1000 }],
    ['maxBytes below the limit', 1000, { maxBytes: 999 }],
    ['the default maxBytes under the default limit', undefined, {}],
  ])('does not warn for %s', (_name, limit, options) => {
    kafka(limit).consumer({ groupId: 'g', ...options })

    expect(logged.filter(({ level }) => level === logLevel.WARN)).toEqual([])
  })
})

describe('Connection > validates hostile size bytes without buffering a body', () => {
  let connection
  afterEach(async () => {
    await connection?.disconnect({ drainTimeout: 0 })
  })
  const prefix = (size) => {
    const bytes = Buffer.alloc(4)
    bytes.writeInt32BE(size)
    return bytes
  }
  it.each([
    ['negative', prefix(-1)],
    ['minus five', prefix(-5)],
    ['two gigabytes', prefix(2147483647)],
    ['HTTP', Buffer.from('HTTP')],
    ['TLS alert', Buffer.from([21, 3, 3, 0])],
  ])(
    'closes for %s before waiting for a body and contains the listener error',
    async (_name, bytes) => {
      const fake = createFakeSocketFactory()
      connection = new Connection(
        connectionOpts({ socketFactory: fake.factory, enforceRequestTimeout: false })
      )
      await connection.connect()
      const socket = fake.state.sockets[0]
      expect(() => socket.emit('data', bytes)).not.toThrow()
      expect(connection.isConnected()).toBe(false)
      expect(socket.destroyed).toBe(true)
    }
  )
})
