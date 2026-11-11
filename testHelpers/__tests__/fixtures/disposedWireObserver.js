'use strict'

const assert = require('node:assert/strict')
const crypto = require('node:crypto')
const fs = require('node:fs')
const net = require('node:net')
const path = require('node:path')
const { setImmediate: turn } = require('node:timers/promises')
const Connection = require('../../../src/network/connection')
const Generation = require('../../../src/producer/generation')
const { ChaosSocket } = require('../../chaosSocket')
const { TelemetryWireObserver } = require('../../telemetryFixtures')

/** @param {Buffer} bytes captured payload @returns {string} its exact hash */
const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

/**
 * Creates a real Socket without dialing a broker; only its wire writes are stubbed.
 * @param {{port:number}} endpoint fixture identity
 * @returns {net.Socket} inert transport with truthful synthetic peer facts
 */
const dial = ({ port }) => {
  const socket = new net.Socket()
  Object.defineProperties(socket, {
    remoteAddress: { value: '127.0.0.1' },
    remotePort: { value: port },
    localAddress: { value: '127.0.0.1' },
    localPort: { value: port + 10000 },
  })
  socket.write = () => true
  return socket
}

/** @returns {object} a scalar clock with no cohort in its closure */
const clock = () => ({
  tick: 0,
  now() {
    return ++this.tick
  },
})

/** @param {Buffer} raw captured bytes @returns {Promise<object>} independent decoder facts */
const decode = async (raw) => ({ parse: 'ok', dataBytes: raw.length, dataSha256: sha(raw) })

/** @returns {object} a logger that keeps no product references */
const logger = () => ({ namespace: logger, debug() {}, warn() {}, error() {} })

/** @param {TelemetryWireObserver} observer retained observer @returns {object} public evidence */
const facts = (observer) => ({
  transcript: observer.readTranscript(),
  checkpoint: observer.checkpoint(),
  accepted: observer.accepted(),
  finals: observer.finals(),
  subscriptions: observer.subscriptions(),
  advertisements: observer.advertisements(),
  ownership: observer.socketOwnership(),
  telemetry: observer.telemetrySockets(),
  undecided: observer.undecided(),
  connects: observer.normalConnects(),
  requests: observer.normalRequests(),
  open: observer.openSockets(),
})

/**
 * Leaves only the observer, a cached terminal error and weak references to retired cohorts.
 * @param {string} directory stimulus and sink directory
 * @returns {Promise<object>} roots and lossless evidence for the collection control
 */
const prepare = async (directory) => {
  const stimulus = JSON.parse(fs.readFileSync(path.join(directory, 'stimulus.json')))
  const observer = new TelemetryWireObserver({
    directory,
    line: '4.3.1',
    owner: 'retired-client',
    clock: clock(),
    dial,
    completeStream: true,
    decoder: decode,
  })
  const connections = []
  const refs = []
  for (const arm of stimulus) {
    const connection = new Connection({
      host: 'localhost',
      port: arm.port,
      logger: logger(),
      socketFactory: ({ onConnect, ...endpoint }) => {
        const inner = observer.socketFactory(endpoint)
        const chaos = new ChaosSocket(inner, endpoint, () => 0.5)
        inner.once('connect', onConnect)
        setImmediate(() => inner.emit('connect'))
        return chaos
      },
      connectionTimeout: 1000,
      requestTimeout: 1000,
      enforceRequestTimeout: false,
    })
    await connection.connect()
    const socket = connection.socket
    refs.push(...[socket.inner, socket, connection].map((value) => new WeakRef(value)))
    // Responses bypass the product parser because these are observer-only wire vectors.
    socket.removeAllListeners('data')
    for (const [request, response] of arm.exchanges) {
      socket.write(Buffer.from(request, 'hex'))
      if (response) socket.inner.emit('data', Buffer.from(response, 'hex'))
    }
    if (arm.fault) {
      observer.declareFault(connections.length + 1)
      socket.write(Buffer.from(arm.fault, 'hex'))
      await connection.disconnect()
    }
    connections.push(connection)
  }
  observer.confirmForegroundBootstrap()
  const decoded = await Promise.all(
    [...observer.accepted(), ...observer.finals()].map((push) => observer.decodeAccepted(push))
  )
  const before = JSON.stringify(facts(observer))
  const generation = new Generation()
  const terminal = Object.assign(new Error('client closed'), { observer })
  generation.close(terminal)
  await observer.closeClient({
    disconnect: async () => {
      for (const connection of connections) await connection.disconnect()
    },
  })
  await observer.socketsClosed()
  await observer.dispose()
  const after = JSON.stringify(facts(observer))
  assert.equal(after, JSON.stringify({ ...JSON.parse(before), open: 0 }))
  observer.assertClean({ allowUnanswered: observer.cancellationKeys() })
  assert.deepEqual(
    await Promise.all(
      [...observer.accepted(), ...observer.finals()].map((push) => observer.decodeAccepted(push))
    ),
    decoded
  )
  const archive = fs.readFileSync(path.join(observer.directory, 'transcript.json'), 'utf8')
  for (const ref of refs.filter((_, index) => index % 3 === 0)) {
    const socket = ref.deref()
    assert.equal(socket.listenerCount('data'), 1)
    assert.equal(socket.listenerCount('connect'), 1)
    assert.equal(socket.listenerCount('close'), 1)
    assert.doesNotThrow(() => socket.emit('error', new Error('late reset')))
  }
  return { observer, generation, terminal, refs, after, archive, decoded }
}

/**
 * CI-only isolated collection control; the positive product root must remain visible.
 * @param {string} directory stimulus directory
 * @returns {Promise<void>} after exact evidence and collection assertions
 */
const run = async (directory) => {
  assert.equal(typeof global.gc, 'function')
  const retained = await prepare(directory)
  const positive = new Connection({
    host: 'localhost',
    port: 19092,
    logger: logger(),
    socketFactory: () => {},
  })
  const positiveRef = new WeakRef(positive)
  for (let round = 0; round < 30; round++) {
    await turn()
    global.gc()
  }
  assert(
    retained.refs.every((ref) => ref.deref() === undefined),
    'Retired socket cohort survived'
  )
  assert.equal(positiveRef.deref(), positive)
  assert.equal(retained.generation.error, retained.terminal)
  assert.equal(retained.generation.guard(), retained.terminal)
  assert.equal(retained.terminal.observer, retained.observer)
  assert.throws(
    () => retained.generation.check(),
    (error) => error === retained.terminal
  )
  assert.equal(JSON.stringify(facts(retained.observer)), retained.after)
  assert.equal(
    fs.readFileSync(path.join(retained.observer.directory, 'transcript.json'), 'utf8'),
    retained.archive
  )
  retained.observer.assertClean({ allowUnanswered: retained.observer.cancellationKeys() })
  await retained.observer.socketsClosed()
  process.stdout.write(
    JSON.stringify({ archive: JSON.parse(retained.archive), decoded: retained.decoded })
  )
}

run(process.argv[2]).catch((error) => {
  process.stderr.write(error.stack + '\n')
  process.exitCode = 1
})
