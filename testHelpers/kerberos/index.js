const { execFileSync } = require('node:child_process')
const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')
const { StandaloneKdc } = require('./standalone')

/**
 * The real process environment. Jest gives a spec a copy of `process.env`, and a write to that
 * copy never reaches `setenv`, so the native Kerberos library reads its own `KRB5_CONFIG` and
 * `KRB5CCNAME` only from here. Outside Jest this is `process.env`.
 */
const NATIVE_ENV = vm.runInThisContext('process.env')

/**
 * Runs one native call and prints its failure, which the product reports without a cause, so a
 * live run shows why it failed.
 * @template T
 * @param {string} method native method name
 * @param {() => T|Promise<T>} call the native call
 * @returns {Promise<T>} the call's result
 * @throws {Error} the native failure, unchanged
 */
const reportingFailure = async (method, call) => {
  try {
    return await call()
  } catch (error) {
    process.stderr.write(`kerberos ${method} failed: ${error.message}\n`)
    throw error
  }
}

/**
 * Talks only to the cluster the run already owns; it cannot start or tear down a cluster. Under a
 * live reservation (`P9_CLUSTER_GUARD` and `P9_CLUSTER_MANIFEST`) the guard performs each operation;
 * with neither set, the job's own Compose cluster does.
 */
class KdcFixture {
  /** Binds the guard's manifest and test credential directory, or the standalone controller.
   * @throws {Error} when only one of the guard and its manifest is supplied
   */
  constructor() {
    this.guard = process.env.P9_CLUSTER_GUARD
    this.manifest = process.env.P9_CLUSTER_MANIFEST
    if (!this.guard !== !this.manifest) throw new Error('Incomplete cluster ownership environment')
    if (this.guard) {
      this.record = JSON.parse(fs.readFileSync(this.manifest, 'utf8'))
    } else {
      this.standalone = new StandaloneKdc()
      this.record = { runDir: this.standalone.directory }
    }
    this.events = []
  }

  /** Requests one allow-listed operation after guard ownership validation, or runs it standalone.
   * @param {string} operation KDC operation name
   * @param {string[]} [args] literal operation arguments
   * @returns {object} validated JSON record from the guard, or the standalone controller's record
   * @throws {Error} invalid reservation, timeout or operation failure
   */
  control(operation, args = []) {
    if (this.standalone) return this.standalone.control(operation, args)
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

  /** Selects a cache before native initialization, without changing keytab state. The selection
   * is made in the real process environment and lasts until the next one.
   * @param {object} ticket validated guard record
   * @returns {void}
   */
  select(ticket) {
    NATIVE_ENV.KRB5_CONFIG = ticket.config
    NATIVE_ENV.KRB5CCNAME = ticket.cache
    delete NATIVE_ENV.KRB5_CLIENT_KTNAME
  }

  /** Captures native inputs/outputs privately for the real-KDC golden.
   * The proxy preserves the binding's receiver and live completion/username getters. A native
   * rejection is printed with its message and rethrown unchanged.
   * @param {object} peer installed native peer
   * @returns {function():void} restoration callback
   */
  observe(peer) {
    const initialize = peer.initializeClient
    const events = this.events
    peer.initializeClient = async function (service, options) {
      const context = events.filter((event) => event.method === 'initialize').length
      const client = await reportingFailure('initializeClient', () =>
        initialize.call(peer, service, options)
      )
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
            const result = await reportingFailure(name, () => target[name](...args))
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
    fs.mkdirSync(this.record.runDir, { recursive: true })
    const target = path.join(this.record.runDir, 'gssapi-native-capture.json')
    fs.writeFileSync(target, JSON.stringify(this.events), { mode: 0o600, flag: 'wx' })
    return target
  }
}

module.exports = KdcFixture
