const { execFileSync } = require('node:child_process')
const fs = require('node:fs')
const path = require('node:path')

/** Talks only to the existing live reservation; it cannot start or tear down a cluster. */
class KdcFixture {
  /** Binds the guard's manifest and test credential directory.
   * @throws {Error} when the guard has not supplied the reservation
   */
  constructor() {
    this.guard = process.env.P9_CLUSTER_GUARD
    this.manifest = process.env.P9_CLUSTER_MANIFEST
    if (!this.guard || !this.manifest) throw new Error('A live reservation is required')
    this.record = JSON.parse(fs.readFileSync(this.manifest, 'utf8'))
    this.events = []
  }

  /** Requests one allow-listed operation after guard ownership validation.
   * @param {string} operation KDC operation name
   * @param {string[]} [args] literal operation arguments
   * @returns {object} validated JSON record from the guard
   * @throws {Error} invalid reservation, timeout or operation failure
   */
  control(operation, args = []) {
    const output = execFileSync(
      'timeout',
      [
        '-k',
        '5s',
        operation === 'gssapi-expired' ? '90s' : '30s',
        'node',
        this.guard,
        'control',
        this.manifest,
        operation,
        ...args,
      ],
      { encoding: 'utf8', env: process.env }
    )
    return JSON.parse(output)
  }

  /** Issues an isolated client cache; the adapter never runs the ticket command.
   * @param {'fresh'|'expired'} name fixed cache selector
   * @returns {object} cache, config, principal, issuedAtMs and expiresAtMs
   * @throws {Error} fixture refusal or failed ticket acquisition
   */
  ticket(name) {
    return this.control('gssapi-ticket', [name])
  }

  /** Selects a cache before native initialization, without changing keytab state.
   * @param {object} ticket validated guard record
   * @returns {void}
   */
  select(ticket) {
    process.env.KRB5_CONFIG = ticket.config
    process.env.KRB5CCNAME = ticket.cache
    delete process.env.KRB5_CLIENT_KTNAME
  }

  /** Captures native inputs/outputs privately for the real-KDC golden.
   * The proxy preserves the binding's receiver and live completion/username getters.
   * @param {object} peer installed native peer
   * @returns {function():void} restoration callback
   */
  observe(peer) {
    const initialize = peer.initializeClient
    const events = this.events
    peer.initializeClient = async function (service, options) {
      const context = events.filter((event) => event.method === 'initialize').length
      const client = await initialize.call(peer, service, options)
      events.push({ context, method: 'initialize', service, principal: options.principal })
      return new Proxy(client, {
        /** Preserves native receivers while recording only exchange methods.
         * @param {object} target native context
         * @param {string|symbol} name property being read
         * @returns {*} original property or an observed method
         */
        get(target, name) {
          if (!['step', 'unwrap', 'wrap'].includes(name)) return Reflect.get(target, name, target)
          return async (...args) => {
            const result = await target[name](...args)
            events.push({ context, method: name, args, result, complete: target.contextComplete })
            return result
          }
        },
      })
    }
    return () => {
      peer.initializeClient = initialize
    }
  }

  /** Writes private capture bytes outside ordinary logs; controller validates before fixture promotion.
   * @returns {string} capture path under the owned run directory
   * @throws {Error} write failure
   */
  capture() {
    const target = path.join(this.record.runDir, 'gssapi-native-capture.json')
    fs.writeFileSync(target, JSON.stringify(this.events), { mode: 0o600, flag: 'wx' })
    return target
  }
}

module.exports = KdcFixture
