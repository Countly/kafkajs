const net = require('net')
const tls = require('tls')
const { once } = require('events')
const { _load } = require('module')
const { installSocketOwners } = require('../socketOwners')
const { captureShareCleanupBaseline } = require('../shareFixtures')

const workerProcess = _load('process')
const registryKey = Symbol.for('kafkajs.test.socketOwners')
const previousRegistry = workerProcess[registryKey]
const previousConnect = net.Socket.prototype.connect
const previousFlag = process.env.KAFKAJS_SOCKET_OWNERS

beforeAll(() => {
  process.env.KAFKAJS_SOCKET_OWNERS = '1'
  installSocketOwners(expect.getState)
})

afterAll(() => {
  if (previousFlag === undefined) delete process.env.KAFKAJS_SOCKET_OWNERS
  else process.env.KAFKAJS_SOCKET_OWNERS = previousFlag
  if (!previousRegistry) {
    net.Socket.prototype.connect = previousConnect
    delete workerProcess[registryKey]
  }
})

describe('socket owner diagnostics', () => {
  test.each(['TCP', 'TLS'])(
    'attributes a %s client to its test and enriches a failed share baseline with live endpoints',
    async (transport) => {
      const server = net.createServer()
      let client
      let peer
      try {
        server.listen(0, '127.0.0.1')
        await once(server, 'listening')
        const accepted = once(server, 'connection')
        const options = { host: '127.0.0.1', port: server.address().port }
        const started = Date.now()
        client = transport === 'TLS' ? tls.connect(options) : net.connect(options)
        const connected = once(client, 'connect')
        ;[peer] = await accepted
        await connected
        const failure = new Error('baseline probe failed')
        const result = await captureShareCleanupBaseline(
          {
            snapshot: async () => {
              throw failure
            },
          },
          { cleanupStack: false }
        ).catch((error) => error)

        expect(result).toBe(failure)
        expect(result.message).toContain('resource baseline')
        expect(result.message).toContain(`JEST_WORKER_ID=${process.env.JEST_WORKER_ID}`)
        expect(result.message).toContain(`pid=${process.pid}`)
        expect(result.message).toContain('testFiles=')
        expect(result.message).toContain('testHelpers/__tests__/socketOwners.spec.js')
        const socketLine = result.message
          .split('\n')
          .find((line) =>
            line.includes(`local=127.0.0.1:${client.localPort} remote=127.0.0.1:${options.port}`)
          )
        expect(socketLine).toContain('destroyed=false readyState=open')
        expect(socketLine).toContain('file="testHelpers/__tests__/socketOwners.spec.js"')
        expect(socketLine).toContain(`test=${JSON.stringify(expect.getState().currentTestName)}`)
        expect(socketLine).toMatch(/stack=.*socketOwners\.spec\.js:\d+:\d+/)
        const time = Number(/time=(\d+)/.exec(socketLine)[1])
        expect(time).toBeGreaterThanOrEqual(started)
        expect(time).toBeLessThanOrEqual(Date.now())
        expect(result.message).toContain(
          `local=127.0.0.1:${peer.localPort} remote=127.0.0.1:${peer.remotePort} destroyed=false readyState=open unregistered`
        )
      } finally {
        const closed = [client, peer].filter(Boolean).map((socket) => {
          const done = once(socket, 'close')
          socket.destroy()
          return done
        })
        await Promise.all(closed)
        if (server.listening) await new Promise((resolve) => server.close(resolve))
      }
    }
  )

  test('keeps one connect hook across Jest module registries', () => {
    const connect = net.Socket.prototype.connect
    jest.isolateModules(() => require('../socketOwners').installSocketOwners(expect.getState))
    expect(net.Socket.prototype.connect).toBe(connect)
  })
})
