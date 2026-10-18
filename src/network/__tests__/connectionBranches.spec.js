const { EventEmitter } = require('events')
const Connection = require('../connection')
const { KafkaJSConnectionError } = require('../../errors')
const { newLogger } = require('testHelpers')

/**
 * A socket factory that records every socket it creates and leaves each one's `onConnect` to the
 * case: `socket.onConnect()` completes that attempt.
 * @returns {{ factory: Function, sockets: EventEmitter[] }}
 */
const createFactory = () => {
  const sockets = []
  const factory = ({ onConnect }) => {
    const socket = new EventEmitter()
    socket.write = () => true
    socket.end = () => {}
    socket.destroy = () => {}
    socket.unref = () => {}
    socket.onConnect = onConnect
    sockets.push(socket)
    return socket
  }
  return { factory, sockets }
}

const build = (socketFactory, options = {}) =>
  new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory,
    clientId: 'test-client',
    connectionTimeout: 200,
    requestTimeout: 200,
    enforceRequestTimeout: false,
    ...options,
  })

describe('Connection > events from an attempt that is no longer current', () => {
  it('ignores a connect that completes after a disconnect and a new attempt replaced its attempt', async () => {
    const { factory, sockets } = createFactory()
    const connection = build(factory)
    const first = connection.connect()
    const firstError = first.catch((e) => e)
    await connection.disconnect()
    const second = connection.connect()

    sockets[0].onConnect()

    expect(connection.isConnected()).toBe(false)
    expect(connection.generation).toBe(0)
    sockets[1].onConnect()
    await expect(second).resolves.toBe(true)
    expect((await firstError).message).toMatch('Connection closed while connecting')
    expect(connection.generation).toBe(1)
    await connection.disconnect()
  })

  describe('after a socket the broker already destroyed was replaced', () => {
    let connection
    let stale

    beforeEach(async () => {
      const { factory, sockets } = createFactory()
      connection = build(factory)
      const connecting = connection.connect()
      sockets[0].onConnect()
      await connecting
      // a closed socket is left alone by disconnect(), listeners included
      stale = sockets[0]
      stale.destroyed = true
      await connection.disconnect()
      const reconnecting = connection.connect()
      sockets[1].onConnect()
      await reconnecting
      jest.spyOn(connection, 'processData')
      jest.spyOn(connection, 'rejectRequests')
    })

    afterEach(async () => {
      await connection.disconnect()
    })

    it('ignores its data', () => {
      stale.emit('data', Buffer.alloc(4))

      expect(connection.processData).not.toHaveBeenCalled()
    })

    it.each([
      ['end', []],
      ['error', [new Error('ECONNRESET')]],
      ['timeout', []],
    ])('ignores its %s, and the new socket stays connected', (event, args) => {
      stale.emit(event, ...args)

      expect(connection.rejectRequests).not.toHaveBeenCalled()
      expect(connection.isConnected()).toBe(true)
    })
  })

  it('rejects a connect that the socket ended before it connected', async () => {
    const { factory, sockets } = createFactory()
    const connection = build(factory)
    const connecting = connection.connect()
    const outcome = connecting.catch((e) => e)
    jest.spyOn(connection, 'rejectRequests')

    sockets[0].emit('end')

    expect(await outcome).toBeInstanceOf(KafkaJSConnectionError)
    expect(connection.isConnected()).toBe(false)
    // there is no request to reject before the connection was ever up
    expect(connection.rejectRequests).not.toHaveBeenCalled()
  })
})

describe('Connection > reporting an authentication failure', () => {
  it('emits the event when no handler was given', () => {
    const instrumentationEmitter = { emit: jest.fn() }
    const connection = build(createFactory().factory, { instrumentationEmitter })

    connection.reportAuthenticationFailure('PLAIN', 'denied')

    expect(instrumentationEmitter.emit).toHaveBeenCalledTimes(1)
    expect(instrumentationEmitter.emit).toHaveBeenCalledWith('network.authentication_failure', {
      broker: '127.0.0.1:9092',
      mechanism: 'PLAIN',
      errorMessage: 'denied',
    })
  })

  it('calls the handler when there is no emitter', () => {
    const onAuthenticationFailure = jest.fn()
    const connection = build(createFactory().factory, { onAuthenticationFailure })

    connection.reportAuthenticationFailure('PLAIN', 'denied')

    expect(onAuthenticationFailure).toHaveBeenCalledWith('denied')
  })
})
