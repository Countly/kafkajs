const net = require('net')
const { EventEmitter } = require('events')
const Encoder = require('../../src/protocol/encoder')
const Connection = require('../../src/network/connection')
const defaultSocketFactory = require('../../src/network/socketFactory')
const { newLogger } = require('testHelpers')
const { createFakeSocketFactory, respondWithFrame } = require('../fakeSocket')
const { createChaosSocketFactory, FrameFilter } = require('../chaosSocket')

/**
 * A request frame as the product writes one: size, api key, version, correlation id, body.
 * @param {number} apiKey
 * @param {number} correlationId
 * @param {number[]} [body]
 * @returns {Buffer}
 */
const requestFrame = (apiKey, correlationId, body = [1, 2, 3]) => {
  const frame = new Encoder()
    .writeInt16(apiKey)
    .writeInt16(0)
    .writeInt32(correlationId)
    .writeBuffer(Buffer.from(body))
  return new Encoder().writeInt32(frame.size()).writeEncoder(frame).buffer
}

/**
 * A response frame as a broker writes one: size, correlation id, body.
 * @param {number} correlationId
 * @param {number[]} [body]
 * @returns {Buffer}
 */
const responseFrame = (correlationId, body = [7, 8]) => {
  const frame = new Encoder().writeInt32(correlationId).writeBuffer(Buffer.from(body))
  return new Encoder().writeInt32(frame.size()).writeEncoder(frame).buffer
}

/** @param {Buffer} bytes @returns {Buffer[]} one-byte chunks */
const bytewise = (bytes) => [...bytes].map((byte) => Buffer.from([byte]))

/** @param {Buffer} bytes @param {number} at @returns {Buffer[]} the two halves */
const splitAt = (bytes, at) => [bytes.subarray(0, at), bytes.subarray(at)]

/** An in-memory stand-in for the real socket a ChaosSocket wraps. */
const fakeInner = () => {
  const inner = new EventEmitter()
  inner.written = []
  inner.destroyed = false
  inner.write = (chunk) => inner.written.push(Buffer.from(chunk))
  inner.end = jest.fn()
  inner.unref = jest.fn()
  inner.setKeepAlive = jest.fn()
  inner.destroy = (error) => {
    if (inner.destroyed) return
    inner.destroyed = true
    if (error) inner.emit('error', error)
    inner.emit('close', Boolean(error))
  }
  return inner
}

/** @returns {{ socket: import('../chaosSocket').ChaosSocket, inner: object, read: Buffer[] }} */
const wrapped = (options) => {
  const inner = fakeInner()
  const factory = createChaosSocketFactory(() => inner, options)
  const socket = factory({ host: 'node', port: 9092, onConnect: () => {} })
  const read = []
  socket.on('data', (chunk) => read.push(chunk))
  socket.on('error', () => {})
  return { socket, inner, read, factory }
}

describe('FrameFilter', () => {
  const frames = [requestFrame(3, 1), requestFrame(26, 2), requestFrame(18, 3)]
  const stream = Buffer.concat(frames)
  const dropEndTxn = () =>
    new FrameFilter({ headerBytes: 12, decide: (header) => header.readInt16BE(4) === 26 })

  test('passes every byte, in order, when nothing is dropped, in one-byte chunks', () => {
    const filter = new FrameFilter({ headerBytes: 12, decide: () => false })

    expect(Buffer.concat(bytewise(stream).flatMap((chunk) => filter.push(chunk)))).toEqual(stream)
  })

  test.each([...Array(stream.length).keys()].slice(1))(
    'drops exactly the selected frame when the stream is split at byte %i',
    (at) => {
      const filter = dropEndTxn()

      const passed = splitAt(stream, at).flatMap((chunk) => filter.push(chunk))

      expect(Buffer.concat(passed)).toEqual(Buffer.concat([frames[0], frames[2]]))
    }
  )

  test('drops the selected frame from a stream delivered one byte at a time', () => {
    const filter = dropEndTxn()

    const passed = bytewise(stream).flatMap((chunk) => filter.push(chunk))

    expect(Buffer.concat(passed)).toEqual(Buffer.concat([frames[0], frames[2]]))
  })

  test('decides a frame shorter than its header at the frame end', () => {
    const decide = jest.fn(() => false)
    const filter = new FrameFilter({ headerBytes: 12, decide })
    const short = Buffer.from([0, 0, 0, 2, 0, 26])

    const passed = bytewise(Buffer.concat([short, frames[0]])).flatMap((c) => filter.push(c))

    expect(decide.mock.calls.map(([header]) => header)).toEqual([short, frames[0].subarray(0, 12)])
    expect(Buffer.concat(passed)).toEqual(Buffer.concat([short, frames[0]]))
  })

  test('stops at a negative size read whole from one chunk, and passes the later chunks', () => {
    const filter = new FrameFilter({ headerBytes: 12, decide: () => true })
    const prefix = Buffer.from([0x80, 0, 0, 0])

    const passed = [prefix, frames[1]].flatMap((chunk) => filter.push(chunk))
    const split = new FrameFilter({ headerBytes: 12, decide: () => true })
    const pieces = [...splitAt(prefix, 2), frames[1]].flatMap((chunk) => split.push(chunk))

    expect(passed).toEqual([prefix, frames[1]])
    expect(pieces).toEqual([prefix, frames[1]])
  })

  test('stops following the stream at a negative size and passes the rest untouched', () => {
    const decide = jest.fn(() => true)
    const filter = new FrameFilter({ headerBytes: 12, decide })
    const bad = Buffer.concat([Buffer.from([0xff, 0xff, 0xff, 0xfb]), frames[1]])

    const passed = [...splitAt(bad, 2), frames[1]].flatMap((chunk) => filter.push(chunk))

    expect(decide).not.toHaveBeenCalled()
    expect(Buffer.concat(passed)).toEqual(Buffer.concat([bad, frames[1]]))
  })
})

describe('ChaosSocket', () => {
  afterEach(() => {
    jest.useRealTimers()
  })

  test('forwards writes, reads and the socket calls the product makes when nothing is armed', () => {
    const { socket, inner, read } = wrapped()
    const frame = requestFrame(3, 1)

    socket.write(frame, 'binary')
    inner.emit('data', responseFrame(1))
    socket.end()
    socket.unref()
    socket.setKeepAlive(true, 1000)

    expect(inner.written).toEqual([frame])
    expect(read).toEqual([responseFrame(1)])
    expect(socket.endpoint).toEqual({ host: 'node', port: 9092 })
    expect(inner.end).toHaveBeenCalledTimes(1)
    expect(inner.unref).toHaveBeenCalledTimes(1)
    expect(inner.setKeepAlive).toHaveBeenCalledWith(true, 1000)
  })

  test('re-emits the real socket events, and reports its destroyed state', () => {
    const { socket, inner } = wrapped()
    const seen = []
    for (const event of ['connect', 'end', 'timeout', 'close']) {
      socket.on(event, () => seen.push(event))
    }

    inner.emit('connect')
    inner.emit('end')
    inner.emit('timeout')
    expect(socket.destroyed).toBe(false)
    socket.destroy()

    expect(seen).toEqual(['connect', 'end', 'timeout', 'close'])
    expect(socket.destroyed).toBe(true)
  })

  test('dropRequests drops only the next matching request, written a byte at a time', () => {
    const { socket, inner } = wrapped()
    const frames = [
      requestFrame(3, 1),
      requestFrame(26, 2),
      requestFrame(18, 3),
      requestFrame(26, 4),
    ]

    socket.dropRequests({ apiKey: 26 })
    bytewise(Buffer.concat(frames)).forEach((chunk) => socket.write(chunk))

    expect(Buffer.concat(inner.written)).toEqual(Buffer.concat([frames[0], frames[2], frames[3]]))
  })

  test('dropRequests counts the frames it drops', () => {
    const { socket, inner } = wrapped()
    const frames = [requestFrame(26, 1), requestFrame(26, 2), requestFrame(26, 3)]

    socket.dropRequests({ apiKey: 26, count: 2 })
    frames.forEach((frame) => socket.write(frame))

    expect(inner.written).toEqual([frames[2]])
  })

  test('dropResponses with an api key drops only the answer to that request', () => {
    const { socket, inner, read } = wrapped()
    socket.write(requestFrame(3, 41))
    socket.write(requestFrame(26, 42))
    socket.write(requestFrame(3, 43))

    socket.dropResponses(1, { apiKey: 26 })
    const answers = [responseFrame(41), responseFrame(42, [9, 9, 9]), responseFrame(43)]
    bytewise(Buffer.concat(answers)).forEach((chunk) => inner.emit('data', chunk))

    expect(Buffer.concat(read)).toEqual(Buffer.concat([answers[0], answers[2]]))
  })

  test('dropResponses without an api key drops the next complete frames, whatever they answer', () => {
    const { socket, inner, read } = wrapped()
    const answers = [responseFrame(1), responseFrame(2), responseFrame(3)]

    socket.dropResponses(2)
    splitAt(Buffer.concat(answers), 5).forEach((chunk) => inner.emit('data', chunk))

    expect(Buffer.concat(read)).toEqual(answers[2])
  })

  test('short frames pass intact while request and response rules are armed', () => {
    const { socket, inner, read } = wrapped()
    const shortRequest = Buffer.from([0, 0, 0, 2, 0, 26])
    const shortResponse = Buffer.from([0, 0, 0, 2, 0, 0])

    socket.dropRequests({ apiKey: 26 })
    socket.dropResponses(1, { apiKey: 26 })
    socket.write(shortRequest)
    inner.emit('data', shortResponse)

    expect(inner.written).toEqual([shortRequest])
    expect(read).toEqual([shortResponse])
  })

  test('dropResponses with thenClose drops the answer, then ends and destroys the socket', () => {
    const { socket, inner, read } = wrapped()
    const ended = jest.fn()
    socket.on('end', ended)
    socket.write(requestFrame(11, 1))
    socket.write(requestFrame(3, 2))

    socket.dropResponses(1, { apiKey: 11, thenClose: true })
    inner.emit('data', responseFrame(1, [1, 2, 3]))
    inner.emit('data', responseFrame(2))

    expect(read).toEqual([])
    expect(ended).toHaveBeenCalledTimes(1)
    expect(inner.destroyed).toBe(true)
  })

  test('dropResponses with thenClose delivers the other frames of the chunk, then ends', () => {
    const { socket, inner, read } = wrapped()
    const ended = jest.fn()
    socket.on('end', ended)
    socket.write(requestFrame(3, 1))
    socket.write(requestFrame(11, 2))
    const answers = [responseFrame(1), responseFrame(2, [9, 9]), responseFrame(3)]

    socket.dropResponses(1, { apiKey: 11, thenClose: true })
    inner.emit('data', Buffer.concat(answers))

    expect(Buffer.concat(read)).toEqual(Buffer.concat([answers[0], answers[2]]))
    expect(ended).toHaveBeenCalledTimes(1)
  })

  test('thenClose waits for the dropped response body before ending', () => {
    const { socket, inner, read } = wrapped()
    const ended = jest.fn()
    socket.on('end', ended)
    socket.write(requestFrame(11, 1))
    socket.dropResponses(1, { apiKey: 11, thenClose: true })
    const answer = responseFrame(1, [1, 2, 3])
    inner.emit('data', answer.subarray(0, 8))
    expect(ended).not.toHaveBeenCalled()
    expect(inner.destroyed).toBe(false)
    inner.emit('data', answer.subarray(8))
    expect(read).toEqual([])
    expect(ended).toHaveBeenCalledTimes(1)
    expect(inner.destroyed).toBe(true)
  })

  test('dropResponses without thenClose leaves the socket open', () => {
    const { socket, inner } = wrapped()
    const ended = jest.fn()
    socket.on('end', ended)
    socket.write(requestFrame(11, 1))

    socket.dropResponses(1, { apiKey: 11 })
    inner.emit('data', responseFrame(1))

    expect(ended).not.toHaveBeenCalled()
    expect(inner.destroyed).toBe(false)
  })

  test('truncateAndClose(0) delivers nothing and closes at the next read', () => {
    const { socket, inner, read } = wrapped()
    const ended = jest.fn()
    socket.on('end', ended)

    socket.truncateAndClose(0)
    inner.emit('data', responseFrame(1))

    expect(read).toEqual([])
    expect(ended).toHaveBeenCalledTimes(1)
    expect(inner.destroyed).toBe(true)
  })

  test('truncateAndClose delivers exactly the bytes asked for, then ends and destroys', () => {
    const { socket, inner, read } = wrapped()
    const ended = jest.fn()
    socket.on('end', ended)
    const answer = responseFrame(1, [1, 2, 3, 4, 5, 6])

    socket.truncateAndClose(6)
    splitAt(answer, 4).forEach((chunk) => inner.emit('data', chunk))
    inner.emit('data', responseFrame(2))

    expect(Buffer.concat(read)).toEqual(answer.subarray(0, 6))
    expect(ended).toHaveBeenCalledTimes(1)
    expect(inner.destroyed).toBe(true)
  })

  test('blackhole swallows both directions, queued deliveries included, and never closes', () => {
    jest.useFakeTimers()
    const { socket, inner, read } = wrapped()
    socket.delay({ read: 50, write: 50 })
    socket.write(requestFrame(3, 1))
    inner.emit('data', responseFrame(1))

    socket.blackhole()
    socket.write(requestFrame(3, 2))
    inner.emit('data', responseFrame(2))
    jest.advanceTimersByTime(1000)

    expect(inner.written).toEqual([])
    expect(read).toEqual([])
    expect(inner.destroyed).toBe(false)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('delay holds each direction for its own time and keeps the order', () => {
    jest.useFakeTimers()
    const { socket, inner, read } = wrapped()

    socket.delay({ read: 100, write: 30 })
    socket.write(requestFrame(3, 1))
    inner.emit('data', responseFrame(1))
    jest.advanceTimersByTime(29)
    expect(inner.written).toEqual([])
    jest.advanceTimersByTime(1)
    expect(inner.written).toEqual([requestFrame(3, 1)])
    socket.delay({ read: 10 })
    inner.emit('data', responseFrame(2))
    jest.advanceTimersByTime(69)
    expect(read).toEqual([])
    jest.advanceTimersByTime(1)

    expect(read).toEqual([responseFrame(1), responseFrame(2)])
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a delayed chunk that comes due later waits on its own timer', () => {
    jest.useFakeTimers()
    const { socket, inner, read } = wrapped()
    socket.delay({ read: 50 })

    inner.emit('data', responseFrame(1))
    jest.advanceTimersByTime(20)
    inner.emit('data', responseFrame(2))
    jest.advanceTimersByTime(30)
    expect(read).toEqual([responseFrame(1)])
    jest.advanceTimersByTime(20)

    expect(read).toEqual([responseFrame(1), responseFrame(2)])
    expect(jest.getTimerCount()).toBe(0)
  })

  test('killAfter destroys the socket with ECONNRESET when its lifetime ends', () => {
    jest.useFakeTimers()
    const { socket, inner } = wrapped()
    const errors = []
    socket.on('error', (error) => errors.push(error))

    socket.killAfter(500)
    jest.advanceTimersByTime(499)
    expect(inner.destroyed).toBe(false)
    jest.advanceTimersByTime(1)

    expect(inner.destroyed).toBe(true)
    expect(errors).toEqual([expect.objectContaining({ code: 'ECONNRESET' })])
  })

  test('a lifetime range is drawn from the seed, so a run repeats', () => {
    jest.useFakeTimers()
    const lifetimes = (seed) => {
      const deaths = []
      const factory = createChaosSocketFactory(fakeInner, { killAfter: [100, 1000], seed })
      for (let index = 0; index < 5; index++) {
        const socket = factory({ host: 'node', port: index })
        socket.on('error', () => {})
        socket.on('close', () => deaths.push([index, Date.now() - start]))
      }
      const start = Date.now()
      jest.advanceTimersByTime(1000)
      return deaths.sort(([a], [b]) => a - b).map(([, at]) => at)
    }

    const first = lifetimes(7)

    expect(first).toEqual(lifetimes(7))
    expect(first).not.toEqual(lifetimes(8))
    expect(first.every((at) => at >= 100 && at <= 1000)).toBe(true)
  })

  test('destroy clears the kill timer and the queued deliveries', () => {
    jest.useFakeTimers()
    const { socket } = wrapped()
    socket.delay({ write: 100 })
    socket.write(requestFrame(3, 1))
    socket.killAfter(1000)

    socket.destroy()

    expect(jest.getTimerCount()).toBe(0)
  })

  test('arm applies to the open sockets of an endpoint and to the ones created later', () => {
    const factory = createChaosSocketFactory(fakeInner)
    const open = factory({ host: 'a', port: 1 })
    const closed = factory({ host: 'a', port: 1 })
    const other = factory({ host: 'b', port: 2 })
    closed.destroy()
    const armed = []

    const disarm = factory.arm({ port: 1 }, (socket) => armed.push(socket))
    const later = factory({ host: 'a', port: 1 })
    factory({ host: 'b', port: 2 })
    disarm()
    factory({ host: 'a', port: 1 })

    expect(armed).toEqual([open, later])
    expect(armed).not.toContain(other)
  })

  test('an armed mode drops the request on each new socket, so it outlives the reconnect', () => {
    const inners = []
    const factory = createChaosSocketFactory(() => {
      const inner = fakeInner()
      inners.push(inner)
      return inner
    })
    factory.arm({ host: 'a' }, (socket) => socket.dropRequests({ apiKey: 26 }))

    for (let correlationId = 1; correlationId <= 2; correlationId++) {
      const socket = factory({ host: 'a', port: 1 })
      socket.write(requestFrame(26, correlationId))
      socket.write(requestFrame(3, correlationId + 10))
    }

    expect(inners.map(({ written }) => written)).toEqual([
      [requestFrame(3, 11)],
      [requestFrame(3, 12)],
    ])
  })

  test('the factory lists every socket it created, oldest first', () => {
    const factory = createChaosSocketFactory(fakeInner)

    const first = factory({ host: 'a', port: 1 })
    const second = factory({ host: 'b', port: 2 })

    first.destroy()
    expect(factory.sockets).toEqual([first, second])
  })

  test('the live-only factory releases closed sockets and preserves fault arming across reconnects', async () => {
    const factory = createChaosSocketFactory(fakeInner, { retainClosed: false })
    let first = factory({ host: 'a', port: 1 })
    const other = factory({ host: 'b', port: 2 })
    const disarm = factory.arm({ host: 'a' }, (socket) => socket.dropRequests({ apiKey: 26 }))
    first.write(requestFrame(26, 1))
    first.write(requestFrame(3, 2))
    expect(first.inner.written).toEqual([requestFrame(3, 2)])
    const released = new WeakRef(first)
    first.destroy()
    expect(factory.sockets).toEqual([other])
    first = null

    const replacement = factory({ host: 'a', port: 1 })
    replacement.write(requestFrame(26, 3))
    replacement.write(requestFrame(3, 4))
    expect(replacement.inner.written).toEqual([requestFrame(3, 4)])
    expect(factory.sockets).toEqual([other, replacement])
    other.inner.destroy()
    expect(factory.sockets).toEqual([replacement])
    disarm()
    const unarmed = factory({ host: 'a', port: 1 })
    unarmed.write(requestFrame(26, 5))
    expect(unarmed.inner.written).toEqual([requestFrame(26, 5)])
    replacement.destroy()
    unarmed.destroy()
    expect(factory.sockets).toEqual([])

    if (typeof global.gc === 'function') {
      for (let i = 0; i < 10; i++) {
        await new Promise((resolve) => setImmediate(resolve))
        global.gc()
      }
      expect(released.deref()).toBeUndefined()
      expect(factory.sockets).toEqual([])
    }
  })

  test('a response it drops leaves a real Connection to its request timeout, which closes it', async () => {
    const { factory: fake, state } = createFakeSocketFactory()
    const chaos = createChaosSocketFactory(fake)
    const connection = new Connection({
      host: 'node',
      port: 9092,
      socketFactory: chaos,
      logger: newLogger(),
      requestTimeout: 100,
      connectionTimeout: 1000,
    })
    const protocol = {
      request: { apiKey: 18, apiVersion: 0, apiName: 'ApiVersions', encode: () => new Encoder() },
      response: { decode: async () => ({}), parse: async (data) => data },
    }

    try {
      await connection.connect()
      chaos.sockets[0].dropResponses(1)
      const sent = connection.send(protocol)
      await new Promise((resolve) => setImmediate(resolve))
      respondWithFrame(state, 0, 0, Buffer.from([0, 0]))

      await expect(sent).rejects.toMatchObject({ name: 'KafkaJSRequestTimeoutError' })
      expect(chaos.sockets[0].destroyed).toBe(true)
    } finally {
      await connection.disconnect()
    }
  })
})

describe('Chaos over real TCP', () => {
  let server
  let clients
  let accepted

  /**
   * Starts a local server and returns its port.
   * @param {(socket: net.Socket) => void} onSocket
   * @returns {Promise<number>}
   */
  const listen = (onSocket) =>
    new Promise((resolve) => {
      server = net.createServer((socket) => {
        accepted.push(socket)
        socket.on('error', () => {})
        onSocket(socket)
      })
      server.listen(0, '127.0.0.1', () => resolve(server.address().port))
    })

  /** @param {Function} factory @param {number} port @returns {Promise<object>} the connected socket */
  const dial = (factory, port) =>
    new Promise((resolve) => {
      const socket = factory({ host: '127.0.0.1', port, onConnect: () => resolve(socket) })
      socket.on('error', () => {})
      clients.push(socket)
    })

  beforeEach(() => {
    clients = []
    accepted = []
  })

  afterEach(async () => {
    clients.forEach((socket) => socket.destroy())
    accepted.forEach((socket) => socket.destroy())
    await new Promise((resolve) => (server ? server.close(resolve) : resolve()))
    server = null
  })

  test('the request-dropping mode drops exactly the selected request and nothing else', async () => {
    const received = []
    let arrived
    const done = new Promise((resolve) => (arrived = resolve))
    const frames = [requestFrame(3, 1), requestFrame(26, 2, [5, 5, 5, 5]), requestFrame(18, 3)]
    const expected = Buffer.concat([frames[0], frames[2]])
    const port = await listen((socket) =>
      socket.on('data', (chunk) => {
        received.push(chunk)
        if (Buffer.concat(received).length >= expected.length) arrived()
      })
    )
    const chaos = createChaosSocketFactory(defaultSocketFactory())
    const socket = await dial(chaos, port)

    socket.dropRequests({ apiKey: 26 })
    for (const chunk of bytewise(Buffer.concat(frames))) {
      socket.write(chunk)
      await new Promise((resolve) => setImmediate(resolve))
    }
    await done

    expect(Buffer.concat(received)).toEqual(expected)
  })

  test('response frames sent in pieces arrive intact around the one it drops', async () => {
    const answers = [responseFrame(1, [1, 1]), responseFrame(2, [2, 2, 2]), responseFrame(3, [3])]
    const port = await listen(async (socket) => {
      for (const chunk of bytewise(Buffer.concat(answers))) {
        socket.write(chunk)
        await new Promise((resolve) => setTimeout(resolve, 1))
      }
    })
    const chaos = createChaosSocketFactory(defaultSocketFactory())
    const socket = await dial(chaos, port)
    const read = []
    const expected = Buffer.concat([answers[0], answers[2]])
    socket.dropResponses(1, { apiKey: 26 })
    socket.write(requestFrame(3, 1))
    socket.write(requestFrame(26, 2))

    await new Promise((resolve) =>
      socket.on('data', (chunk) => {
        read.push(chunk)
        if (Buffer.concat(read).length >= expected.length) resolve()
      })
    )

    expect(Buffer.concat(read)).toEqual(expected)
    expect(read.length).toBeGreaterThan(1)
  })
})

describe('KAFKAJS_TEST_CHAOS', () => {
  const load = (value) => {
    const previous = process.env.KAFKAJS_TEST_CHAOS
    try {
      if (value === undefined) delete process.env.KAFKAJS_TEST_CHAOS
      else process.env.KAFKAJS_TEST_CHAOS = value
      let helpers
      jest.isolateModules(() => {
        helpers = require('../index')
      })
      return helpers.connectionOpts().socketFactory
    } finally {
      if (previous === undefined) delete process.env.KAFKAJS_TEST_CHAOS
      else process.env.KAFKAJS_TEST_CHAOS = previous
    }
  }

  test('=1 builds the shared socket factory through the chaos wrapper', () => {
    expect(load('1').sockets).toEqual([])
  })

  test.each([undefined, '0'])('%s keeps the plain shared socket factory', (value) => {
    expect(load(value).sockets).toBeUndefined()
  })
})
