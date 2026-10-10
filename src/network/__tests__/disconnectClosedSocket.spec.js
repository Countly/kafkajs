const net = require('net')
const Connection = require('../connection')
const defaultSocketFactory = require('../socketFactory')
const { newLogger } = require('testHelpers')

/** More `disconnect()` calls than Node lets an emitter gather listeners for one event unwarned. */
const DISCONNECTS = 11

/**
 * A TCP server on a free local port that closes every connection as soon as it is accepted.
 * @returns {Promise<net.Server>}
 */
const closingServer = () =>
  new Promise((resolve) => {
    const server = net.createServer((socket) => socket.end())
    server.listen(0, '127.0.0.1', () => resolve(server))
  })

describe('Network > Connection > disconnect() after the broker closed the socket', () => {
  let server

  beforeEach(async () => {
    server = await closingServer()
  })

  afterEach(async () => {
    await new Promise((resolve) => server.close(resolve))
  })

  test('tears the socket down once, so repeated calls leave no listener on it', async () => {
    const warnings = []
    const onWarning = (warning) => warnings.push(warning.name)
    process.on('warning', onWarning)
    const connection = new Connection({
      host: '127.0.0.1',
      port: server.address().port,
      logger: newLogger(),
      socketFactory: defaultSocketFactory(),
      clientId: 'test-client',
      connectionTimeout: 1000,
      requestTimeout: 1000,
    })

    await connection.connect()
    const { socket } = connection
    await new Promise((resolve) => (socket.destroyed ? resolve() : socket.once('close', resolve)))
    for (let call = 0; call < DISCONNECTS; call++) {
      await connection.disconnect()
    }
    await new Promise((resolve) => setImmediate(resolve))
    process.off('warning', onWarning)

    expect(socket.destroyed).toBe(true)
    expect(socket.listenerCount('connect')).toBe(0)
    expect(warnings).not.toContain('MaxListenersExceededWarning')
    expect(connection.isConnected()).toBe(false)
  })
})
