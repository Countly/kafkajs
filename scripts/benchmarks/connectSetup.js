const assert = require('node:assert/strict')
const net = require('node:net')
const os = require('node:os')
const { once } = require('node:events')
const Broker = require('../../src/broker')
const ConnectionPool = require('../../src/network/connectionPool')
const { createLogger } = require('../../src/loggers')
const { readRequestHeader } = require('../../testHelpers/fakeSocket')
const pair = require('../../testHelpers/wire/apiVersionsV5')
const { createResourceProbe } = require('../../testHelpers/resourceProbe')

/** Measures real ready episodes while keeping destruction outside the measured interval. */
class SetupBenchmark {
  /**
   * @param {object} [options]
   * @param {number} [options.episodes=2000] smaller only for the driver unit spec
   * @param {boolean} [options.control=false] adds one measured millisecond per episode
   * @param {boolean} [options.reuse=false] negative control: healthy no-op connections
   */
  constructor({ episodes = 2000, control = false, reuse = false } = {}) {
    this.episodes = episodes
    this.control = control
    this.reuse = reuse
    this.accepted = new Set()
    this.open = new Set()
    this.clients = new Set()
    this.negotiations = 0
    this.created = 0
    this.destroyed = 0
    this.error = null
  }

  /**
   * Reads complete frames independently of the product frame accumulator, accepting only the
   * ApiVersions v5 a Kafka 4.4 broker is first asked, so each episode is one round trip.
   * @param {import('node:net').Socket} socket accepted server socket
   * @returns {void}
   */
  accept(socket) {
    this.accepted.add(socket)
    this.open.add(socket)
    socket.on('close', () => this.open.delete(socket))
    socket.on('error', (error) => {
      this.error = error
    })
    let pending = Buffer.alloc(0)
    socket.on('data', (chunk) => {
      try {
        pending = Buffer.concat([pending, chunk])
        while (pending.length >= 4 && pending.length >= pending.readInt32BE(0) + 4) {
          const length = pending.readInt32BE(0) + 4
          assert(length >= 4)
          const frame = pending.subarray(0, length)
          pending = pending.subarray(length)
          const header = readRequestHeader(frame)
          assert.equal(header.apiKey, 18)
          assert.equal(header.apiVersion, 5)
          pair.readRequest(frame.subarray(header.byteLength))
          this.negotiations++
          const body = pair.buildResponse({
            apiKeys: [{ apiKey: 18, minVersion: 5, maxVersion: 5 }],
          })
          const response = Buffer.alloc(8 + body.length)
          response.writeInt32BE(4 + body.length)
          response.writeInt32BE(header.correlationId, 4)
          body.copy(response, 8)
          socket.write(response)
        }
      } catch (error) {
        this.error = error
        socket.destroy()
      }
    })
  }

  /** @returns {Broker} a fresh real pool using a loopback socket factory */
  broker() {
    const logger = createLogger({ level: 0, logCreator: () => () => {} })
    const socketFactory = ({ host, port, onConnect }) => {
      this.created++
      const socket = net.connect({ host, port }, onConnect)
      this.clients.add(socket)
      socket.on('close', () => {
        this.destroyed++
        this.clients.delete(socket)
      })
      return socket
    }
    return new Broker({
      logger,
      connectionPool: new ConnectionPool({
        logger,
        host: '127.0.0.1',
        port: this.port,
        socketFactory,
        connectionTimeout: 3000,
        requestTimeout: 3000,
      }),
    })
  }

  /**
   * Completes both ends' closure before the next measured episode.
   * @param {Broker} broker measured broker
   * @returns {Promise<void>}
   */
  async cleanup(broker) {
    const closed = [...this.clients, ...this.open].map((socket) =>
      socket.closed ? Promise.resolve() : once(socket, 'close')
    )
    await broker.disconnect({ drainTimeout: 0 })
    await Promise.all(closed)
  }

  /**
   * @returns {Promise<object>} metric and actual-work/resource counters
   * @throws {Error} when work, framing or resource assertions fail
   */
  async run() {
    const probe = createResourceProbe({ activeResources: true })
    const before = await probe.snapshot()
    const loadBefore = os.loadavg()
    const server = net.createServer((socket) => this.accept(socket))
    let broker
    let elapsedNs = 0n
    await new Promise((resolve, reject) => {
      server.once('error', reject)
      server.listen(0, '127.0.0.1', resolve)
    })
    this.port = server.address().port
    try {
      for (let i = 0; i < this.episodes; i++) {
        broker = broker && this.reuse ? broker : this.broker()
        const start = process.hrtime.bigint()
        if (this.control) await new Promise((resolve) => setTimeout(resolve, 1))
        await broker.connect()
        const duration = process.hrtime.bigint() - start
        assert(broker.isConnected())
        elapsedNs += duration
        if (!this.reuse) {
          await this.cleanup(broker)
        }
      }
    } finally {
      if (broker) await broker.disconnect({ drainTimeout: 0 })
      for (const socket of this.open) socket.destroy()
      await new Promise((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve()))
      )
      await new Promise((resolve) => setImmediate(resolve))
      await new Promise((resolve) => setImmediate(resolve))
    }
    if (this.error) throw this.error
    assert.equal(this.created, this.episodes, 'distinct client establishments')
    assert.equal(this.accepted.size, this.episodes, 'distinct server establishments')
    assert.equal(this.negotiations, this.episodes, 'ApiVersions negotiations')
    assert.equal(this.created, this.destroyed, 'client sockets after cleanup')
    assert.equal(this.open.size, 0, 'server sockets after cleanup')
    const after = await probe.snapshot()
    const timers = (after.activeResources.Timeout || 0) - (before.activeResources.Timeout || 0)
    assert(timers <= 0, 'timers after cleanup')
    return {
      usPerSetup: Number(elapsedNs) / 1000 / this.episodes,
      episodes: this.episodes,
      establishments: this.created,
      negotiations: this.negotiations,
      sockets: this.created - this.destroyed,
      timers: Math.max(0, timers),
      baselineTimers: before.activeResources.Timeout || 0,
      loadBefore,
      loadAfter: os.loadavg(),
      node: process.version,
      control: this.control,
    }
  }
}

/** Runs the fixed 2000-episode CLI. @returns {Promise<void>} @throws {Error} failed assertions */
const main = async () => {
  const args = process.argv.slice(2)
  assert(
    args.length === 0 ||
      (args.length === 2 && args[0] === '--control' && ['0', '1'].includes(args[1])),
    'usage: connectSetup.js [--control 0|1]'
  )
  const result = await new SetupBenchmark({ control: args[1] === '1' }).run()
  assert.equal(result.baselineTimers, 0, 'CLI timer baseline')
  process.stdout.write(`${JSON.stringify(result)}\n`)
}

if (require.main === module)
  main().catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })
module.exports = { SetupBenchmark, main }
