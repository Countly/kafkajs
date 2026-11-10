const net = require('net')
const tls = require('tls')
const { once } = require('events')
const { readFileSync } = require('fs')
const path = require('path')
const vm = require('vm')
const { _load, createRequire } = require('module')
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

  test('keeps the hook and retained ownership data outside file realms when files change', () => {
    const savedRegistry = workerProcess[registryKey]
    const savedConnect = net.Socket.prototype.connect
    const native = _load('vm').runInThisContext('({ Function, Object, Array, WeakMap })')
    const helperPath = require.resolve('../socketOwners')
    const source = readFileSync(helperPath, 'utf8')
    const nativeRequire = createRequire(helperPath)
    const files = ['first.spec.js', 'second.spec.js', 'third.spec.js'].map((file) =>
      path.join(__dirname, file)
    )
    try {
      delete workerProcess[registryKey]
      net.Socket.prototype.connect = previousConnect
      let connect
      for (const file of files) {
        const context = vm.createContext({
          require: nativeRequire,
          module: { exports: {} },
          __dirname: path.dirname(helperPath),
          process: { env: { KAFKAJS_SOCKET_OWNERS: '1' } },
        })
        vm.runInContext(
          `${source}\nmodule.exports.installSocketOwners(() => ({
            testPath: ${JSON.stringify(file)}, currentTestName: 'realm test'
          }))`,
          context,
          { filename: helperPath }
        )
        connect ||= net.Socket.prototype.connect
        expect(net.Socket.prototype.connect).toBe(connect)
        const registry = workerProcess[registryKey]
        expect(registry.getState).toBeInstanceOf(vm.runInContext('Function', context))
      }

      const registry = workerProcess[registryKey]
      // File-realm functions or containers retain their realm even after its reader is replaced.
      expect(connect).toBeInstanceOf(native.Function)
      expect(registry).toBeInstanceOf(native.Object)
      expect(registry.enterFile).toBeInstanceOf(native.Function)
      expect(registry.record).toBeInstanceOf(native.Function)
      expect(registry.describe).toBeInstanceOf(native.Function)
      expect(registry.owners).toBeInstanceOf(native.WeakMap)
      expect(registry.testFiles).toBeInstanceOf(native.Array)
      expect(registry.testFiles).toEqual(files)

      const socket = new net.Socket()
      try {
        registry.record(socket, registry.record)
        const owner = registry.owners.get(socket)
        expect(owner).toBeInstanceOf(native.Object)
        expect(owner.stack).toBeInstanceOf(native.Array)
        expect(owner).toMatchObject({ file: files[2], test: 'realm test' })
        expect(
          Object.values(owner)
            .flat()
            .every((value) => typeof value === 'string' || typeof value === 'number')
        ).toBe(true)
      } finally {
        socket.destroy()
      }
    } finally {
      net.Socket.prototype.connect = savedConnect
      workerProcess[registryKey] = savedRegistry
    }
  })
})
