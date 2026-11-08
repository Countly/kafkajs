const { EventEmitter } = require('events')
const Encoder = require('../../src/protocol/encoder')
const {
  createFakeSocketFactory,
  socketCounts,
  respondWithFrame,
  readRequestHeader,
} = require('../fakeSocket')

const dial = (factory, host = '10.0.0.1', port = 9092) =>
  new Promise((resolve) => {
    const socket = factory({ host, port, onConnect: () => resolve(socket) })
  })

/** Lets every microtask already queued run. */
const microtasks = () => Promise.resolve()

describe('createFakeSocketFactory', () => {
  test('hands every socket the endpoint the product dialled, and keeps each one', async () => {
    const { factory, state } = createFakeSocketFactory()

    const first = await dial(factory, '10.0.0.1', 9092)
    const second = await dial(factory, '10.0.0.2', 9093)

    expect(first.endpoint).toEqual({ host: '10.0.0.1', port: 9092 })
    expect(second.endpoint).toEqual({ host: '10.0.0.2', port: 9093 })
    expect(state.sockets).toEqual([first, second])
    expect(state.socket).toBe(second)
  })

  test('records each write on its own socket and in the shared list, as a copy', async () => {
    const { factory, state } = createFakeSocketFactory()
    const first = await dial(factory)
    const second = await dial(factory)
    const bytes = Buffer.from([1, 2, 3])

    first.write(bytes)
    second.write(Buffer.from([4]))
    bytes[0] = 9

    expect(first.writes).toEqual([Buffer.from([1, 2, 3])])
    expect(second.writes).toEqual([Buffer.from([4])])
    expect(state.writes).toEqual([Buffer.from([1, 2, 3]), Buffer.from([4])])
  })

  test('accepts end() and unref() as the product calls them, and does nothing with them', async () => {
    const { factory, state } = createFakeSocketFactory()
    const socket = await dial(factory)

    expect(() => {
      socket.end()
      socket.unref()
    }).not.toThrow()
    expect(socket.destroyed).toBe(false)
    expect(state.writes).toEqual([])
  })

  describe('destroy', () => {
    test('marks the socket destroyed at once and emits close, once, a microtask later', async () => {
      const { factory } = createFakeSocketFactory()
      const socket = await dial(factory)
      const closed = jest.fn()
      socket.on('close', closed)
      expect(socket.destroyed).toBe(false)

      expect(socket.destroy()).toBe(socket)

      expect(socket.destroyed).toBe(true)
      expect(closed).not.toHaveBeenCalled()
      await microtasks()
      expect(closed).toHaveBeenCalledTimes(1)
      expect(closed).toHaveBeenCalledWith(false)
    })

    test('with an error emits error and then close with hadError true', async () => {
      const { factory } = createFakeSocketFactory()
      const socket = await dial(factory)
      const events = []
      socket.on('error', (error) => events.push(['error', error.message]))
      socket.on('close', (hadError) => events.push(['close', hadError]))

      socket.destroy(new Error('reset'))
      await microtasks()

      expect(events).toEqual([
        ['error', 'reset'],
        ['close', true],
      ])
    })

    test('a second destroy emits nothing more', async () => {
      const { factory } = createFakeSocketFactory()
      const socket = await dial(factory)
      const closed = jest.fn()
      socket.on('close', closed)

      socket.destroy()
      socket.destroy(new Error('again'))
      await microtasks()

      expect(closed).toHaveBeenCalledTimes(1)
    })

    test('still records a write that comes after it, so a test can see it', async () => {
      const { factory, state } = createFakeSocketFactory()
      const socket = await dial(factory)
      socket.destroy()

      socket.write(Buffer.from([7]))

      expect(socket.destroyed).toBe(true)
      expect(socket.writes).toEqual([Buffer.from([7])])
      expect(state.writes).toHaveLength(1)
    })
  })
})

describe('socketCounts', () => {
  test('counts the sockets opened and the ones destroyed', async () => {
    const { factory, state } = createFakeSocketFactory()
    expect(socketCounts(state)).toEqual({ created: 0, destroyed: 0 })

    const first = await dial(factory)
    await dial(factory)
    first.destroy()

    expect(socketCounts(state)).toEqual({ created: 2, destroyed: 1 })
  })
})

describe('createFakeSocketFactory with bounded capture', () => {
  const bounded = (options = {}) => createFakeSocketFactory({ bounded: options })

  test('keeps the default factory unchanged: every socket and write is still retained', async () => {
    const { factory, state } = createFakeSocketFactory()
    const socket = await dial(factory)
    socket.write(Buffer.from([1, 2]))
    socket.destroy()
    await microtasks()
    await microtasks()

    expect(state.bounded).toBe(false)
    expect(state.sockets).toEqual([socket])
    expect(state.writes).toEqual([Buffer.from([1, 2])])
    expect(socket.writes).toEqual([Buffer.from([1, 2])])
    expect({ issued: state.issued, closed: state.closed, writeCount: state.writeCount }).toEqual({
      issued: 1,
      closed: 1,
      writeCount: 1,
    })
  })

  test('drops a closed socket only after its close callbacks have run', async () => {
    const closed = []
    const { factory, state } = bounded({ onClosed: (socket) => closed.push(socket) })
    const socket = await dial(factory)
    const events = []
    socket.on('error', () => events.push('error'))
    socket.on('close', () => events.push(`close live=${state.sockets.length}`))

    socket.destroy(new Error('lost'))
    expect(state.sockets).toEqual([socket])
    await microtasks()
    await microtasks()

    expect(events).toEqual(['error', 'close live=1'])
    expect(state.sockets).toEqual([])
    expect(closed).toEqual([socket])
    expect(state.closed).toBe(1)
  })

  test('keeps counters, not writes, and counts a write after close as a scalar fact', async () => {
    const { factory, state } = bounded()
    const socket = await dial(factory)
    socket.write(Buffer.alloc(3))
    socket.destroy()
    await microtasks()
    await microtasks()
    socket.write(Buffer.alloc(5))

    expect(state.writes).toEqual([])
    expect(socket.writes).toEqual([])
    expect({
      count: state.writeCount,
      bytes: state.writeBytes,
      afterClose: state.writesAfterClose,
    }).toEqual({ count: 2, bytes: 8, afterClose: 1 })
  })

  test('numbers sockets by issued counters, so pruning never reuses a generation', async () => {
    const { factory, state } = bounded()
    const seen = []
    for (let i = 0; i < 5; i++) {
      const socket = await dial(factory, 'a', 9092)
      seen.push(state.issuedByEndpoint.get('a:9092'))
      socket.destroy()
      await microtasks()
      await microtasks()
    }

    expect(seen).toEqual([1, 2, 3, 4, 5])
    expect(state.sockets).toEqual([])
    expect(state.issued).toBe(5)
    expect(socketCounts(state)).toEqual({ created: 5, destroyed: 5 })
  })

  test('counts a destroyed socket whose close has not been emitted yet', async () => {
    const { factory, state } = bounded()
    const first = await dial(factory)
    await dial(factory)
    first.destroy()

    expect(socketCounts(state)).toEqual({ created: 2, destroyed: 1 })
  })

  test('refuses a socket past the live bound, reports it and builds nothing', async () => {
    const overflow = jest.fn()
    const { factory, state } = bounded({ maxLiveSockets: 2, onOverflow: overflow })
    await dial(factory)
    await dial(factory)

    expect(() => factory({ host: 'a', port: 1, onConnect: () => {} })).toThrow(
      'Fake socket capture overflow: more than 2 live sockets'
    )
    expect(overflow).toHaveBeenCalledTimes(1)
    expect(state.issued).toBe(2)
    expect(state.sockets).toHaveLength(2)
  })

  test('admits a new socket once a closed one has been dropped', async () => {
    const { factory, state } = bounded({ maxLiveSockets: 1 })
    const first = await dial(factory)
    first.destroy()
    await microtasks()
    await microtasks()

    await expect(dial(factory)).resolves.toBeInstanceOf(EventEmitter)
    expect(state.issued).toBe(2)
  })
})

describe('respondWithFrame', () => {
  const framed = (headerVersion, body, headerTags) => {
    const state = { socket: new EventEmitter() }
    const chunks = []
    state.socket.on('data', (chunk) => chunks.push(chunk))

    respondWithFrame(state, 7, headerVersion, body, headerTags)

    return chunks
  }

  test('frames a classic response as size, correlation id and body, in one chunk', () => {
    expect(framed(0, Buffer.from([9, 9]))).toEqual([Buffer.from([0, 0, 0, 6, 0, 0, 0, 7, 9, 9])])
  })

  test('puts the empty tagged-field buffer after the correlation id of a flexible header', () => {
    expect(framed(1, Buffer.from([9]))).toEqual([Buffer.from([0, 0, 0, 6, 0, 0, 0, 7, 0, 9])])
  })
})

describe('readRequestHeader', () => {
  const request = (apiKey, apiVersion) =>
    new Encoder()
      .writeInt32(0)
      .writeInt16(apiKey)
      .writeInt16(apiVersion)
      .writeInt32(5)
      .writeString('client')
      .writeBuffer(Buffer.from([1, 2, 3]))

  test('reads a classic header and says where the body starts', () => {
    const frame = request(3, 1).buffer

    expect(readRequestHeader(frame)).toEqual({
      apiKey: 3,
      apiVersion: 1,
      correlationId: 5,
      clientId: 'client',
      byteLength: 4 + 2 + 2 + 4 + 2 + 6,
    })
  })

  test('skips the tagged-field buffer of a flexible header', () => {
    const header = new Encoder()
      .writeInt32(0)
      .writeInt16(3)
      .writeInt16(13)
      .writeInt32(5)
      .writeString('client')
      .writeUVarInt(0)
    const frame = Buffer.concat([header.buffer, Buffer.from([1, 2, 3])])

    expect(readRequestHeader(frame)).toMatchObject({
      apiKey: 3,
      apiVersion: 13,
      byteLength: 4 + 2 + 2 + 4 + 2 + 6 + 1,
    })
  })
})
