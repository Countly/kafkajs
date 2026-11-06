'use strict'
const assert = require('node:assert/strict')
const { EventEmitter } = require('node:events')
const { loadavg, cpus } = require('node:os')
const Connection = require('../../src/network/connection')
const Encoder = require('../../src/protocol/encoder')
const { createLogger, LEVELS } = require('../../src/loggers')
const LoggerConsole = require('../../src/loggers/console')

/**
 * Measures completed pairs through the real prepare, send, queue and frame-fulfillment
 * path. Counts request identities and preparation entries on either tree, without
 * requiring candidate-only fields. The fixture owns one socket and one reusable
 * response buffer. Its work is identical in every arm. Production's added stamps
 * allocate no per-pair objects; the existing async protocol itself still allocates.
 */
class RequestQueueStamp {
  /**
   * @param {object} [options] untimed unit-smoke or session controls
   * @param {number} [options.pairs=2000000] actual completed pairs
   * @param {0|1} [options.control=0] ten extra clock calls per pair when enabled
   * @param {number} [options.heapBudget=5242880] the most the heap may grow over a run, in bytes;
   *   meaningful only with a real `gc`
   * @returns {RequestQueueStamp} an unopened driver
   * @throws {Error} for an invalid count or control
   */
  constructor({ pairs = 2000000, control = 0, heapBudget = 5 * 1024 * 1024 } = {}) {
    assert(Number.isInteger(pairs) && pairs > 0)
    assert(control === 0 || control === 1)
    this.pairs = pairs
    this.control = control
    this.heapBudget = heapBudget
    this.encoded = 0
    this.prepared = 0
    this.written = 0
    this.fulfilled = 0
    this.lastId = -1
    this.extraClocks = 0
    this.frame = Buffer.alloc(8)
    this.frame.writeInt32BE(4)
    this.socket = new EventEmitter()
    this.socket.destroyed = false
    this.socket.unref = () => {}
    this.socket.destroy = () => {
      this.socket.destroyed = true
    }
    this.deliver = () => this.socket.emit('data', this.frame)
    this.socket.write = (frame) => {
      const id = frame.readInt32BE(8)
      assert.equal(id, this.lastId + 1)
      this.lastId = id
      this.written++
      this.frame.writeInt32BE(id, 4)
      queueMicrotask(this.deliver)
      return true
    }
    const logger = createLogger({ level: LEVELS.NOTHING, logCreator: LoggerConsole })
    this.connection = new Connection({
      host: 'localhost',
      port: 9092,
      logger,
      clientId: 'queue-benchmark',
      connectionTimeout: 1000,
      requestTimeout: 30000,
      enforceRequestTimeout: false,
      connectionsMaxIdle: 0,
      socketFactory: ({ onConnect }) => {
        queueMicrotask(onConnect)
        return this.socket
      },
    })
    const prepareKey = Object.getOwnPropertySymbols(Connection.prototype).find(
      (key) => key.description === 'private:Connection:prepare'
    )
    assert(prepareKey, 'the real preparation boundary must be present')
    const prepare = this.connection[prepareKey].bind(this.connection)
    this.connection[prepareKey] = (options) => {
      this.prepared++
      return prepare(options)
    }
    const queue = this.connection.requestQueue
    const fulfill = queue.fulfillRequest.bind(queue)
    queue.fulfillRequest = (response) => {
      assert.equal(response.correlationId, this.fulfilled)
      fulfill(response)
      this.fulfilled++
      if (this.fulfilled === this.pairs) this.finishedAt = process.hrtime.bigint()
    }
    this.protocol = {
      request: {
        apiKey: 18,
        apiVersion: 0,
        apiName: 'ApiVersions',
        encode: async () => {
          this.encoded++
          return new Encoder()
        },
      },
      response: { decode: async () => ({}), parse: async (value) => value },
    }
  }

  /**
   * Runs once; measurement starts before preparation and stops at the last matching
   * fulfillment. Final parse and cleanup are outside the measured interval.
   * @returns {Promise<object>} numeric metrics, counts, load and heap/resource readings
   * @throws {Error} for missing work, duplicate identities, retention or open resources
   */
  async run() {
    assert.equal(typeof globalThis.gc, 'function', 'run with --expose-gc')
    await this.connection.connect()
    globalThis.gc()
    const heapBefore = process.memoryUsage().heapUsed
    const beforeLoad = loadavg()[0]
    const startedAt = process.hrtime.bigint()
    try {
      for (let i = 0; i < this.pairs; i++) {
        if (this.control)
          for (let n = 0; n < 10; n++) {
            Date.now()
            this.extraClocks++
          }
        await this.connection.send(this.protocol)
      }
      assert.equal(this.encoded, this.pairs)
      assert.equal(this.prepared, this.pairs)
      assert.equal(this.written, this.pairs)
      assert.equal(this.fulfilled, this.pairs)
      assert.equal(this.extraClocks, this.control * 10 * this.pairs)
      assert.equal(this.connection.requestQueue.pending.length, 0)
      assert.equal(this.connection.requestQueue.inflight.size, 0)
      globalThis.gc()
      const heapGrowth = process.memoryUsage().heapUsed - heapBefore
      assert(heapGrowth < this.heapBudget, `heap grew by at least ${this.heapBudget} bytes`)
      return {
        nsPerPair: Number(this.finishedAt - startedAt) / this.pairs,
        pairs: this.pairs,
        encoded: this.encoded,
        prepared: this.prepared,
        written: this.written,
        fulfilled: this.fulfilled,
        extraClocks: this.extraClocks,
        heapGrowth,
        loadBefore: beforeLoad,
        loadAfter: loadavg()[0],
        cpuCount: cpus().length,
      }
    } finally {
      await this.connection.disconnect({ drainTimeout: 0 })
      assert(this.socket.destroyed)
      assert.equal(this.connection.socket, null)
      assert.equal(this.connection.requestQueue.throttleCheckTimeoutId, null)
      assert.equal(this.connection.idleTimer ?? null, null)
    }
  }
}

if (require.main === module) {
  const args = process.argv.slice(2)
  assert(
    args.length === 0 || (args.length === 2 && args[0] === '--control' && /^[01]$/.test(args[1]))
  )
  new RequestQueueStamp({ control: Number(args[1] || 0) }).run().then(
    (result) => process.stdout.write(`${JSON.stringify(result)}\n`),
    (error) => {
      process.stderr.write(`${error.stack}\n`)
      process.exitCode = 1
    }
  )
}
module.exports = RequestQueueStamp
