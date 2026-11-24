const Broker = require('../src/broker')
const { lookup } = require('../src/protocol/requests')
const { KafkaJSConnectionError } = require('../src/errors')

const quiet = { namespace: () => quiet, debug() {}, info() {}, warn() {}, error() {} }

/**
 * A real Broker over a pool that, like the request queue, runs each request's final pre-write
 * guard and then writes it (`onWrite`) in one step, and answers with the next of `answers`:
 * - `'ok'`: decoded `{}`, returned
 * - `'lost'`: written, then the connection fails and no answer is decoded
 * - `'dial'`: fails before anything is written
 * - an Error: decoded `{}`, then thrown, as a parsed broker error is
 * - `{ decoded, error }`: `decoded` is decoded (the request's hooks see it), then `error` is
 *   thrown when given, else `decoded` is returned
 *
 * `'ok'` is used once `answers` runs out. The guard is asked once more after each write, as the
 * response guard would.
 * @param {Array<string|Error|{ decoded: object, error?: Error }>} answers one per send
 * @param {object} options
 * @param {object} options.versions the broker's advertised ranges
 * @param {Promise<void>} [options.hold] awaited before each connection is handed out, after the
 *   request was built
 * @param {boolean} [options.encode=false] encode each request before its final guard and keep the
 *   bytes of each written one in `frames`
 * @param {number} [options.nodeId=1]
 * @returns {{ broker: Broker, pool: object, written: string[], frames: Buffer[],
 *   afterWrite: Array<Error|null>, lost: Error }}
 */
const latchedBroker = (answers, { versions, hold = null, encode = false, nodeId = 1 }) => {
  const written = []
  const frames = []
  const afterWrite = []
  const lost = new KafkaJSConnectionError('Connection closed')
  const connection = { isConnected: () => true }
  const pool = {
    generation: 0,
    pool: [],
    setupPolicy: { timeoutCeiling: 30000, waitCeiling: 1000 },
    getConnectionByRequest: jest.fn(async () => {
      if (hold) await hold
      return connection
    }),
    send: jest.fn(async (protocolRequest, _connection, options) => {
      const frame = encode ? (await protocolRequest.request.encode()).buffer : null
      const refusal = options?.guard?.()
      if (refusal) throw refusal
      const answer = answers.shift() ?? 'ok'
      if (answer === 'dial') throw new KafkaJSConnectionError('Connection refused')
      protocolRequest.onWrite?.()
      written.push(protocolRequest.request.apiName)
      if (encode) frames.push(frame)
      afterWrite.push(options?.guard?.() ?? null)
      if (answer === 'lost') throw lost
      const decoded = answer?.decoded ?? {}
      protocolRequest.onDecoded?.(decoded)
      if (answer instanceof Error) throw answer
      if (answer?.error) throw answer.error
      return decoded
    }),
    destroy: jest.fn(),
  }
  const broker = new Broker({ connectionPool: pool, logger: quiet, nodeId, versions })
  broker.lookupRequest = lookup(versions)
  broker.versionsGeneration = 0
  return { broker, pool, written, frames, afterWrite, lost }
}

module.exports = { latchedBroker }
