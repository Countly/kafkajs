const apiKeys = require('../protocol/requests/apiKeys')
const Connection = require('./connection')
const { KafkaJSConnectionError } = require('../errors')

// Floor for the post-authentication-failure blackout, applied even when authenticationTimeout
// is unset or shorter, so a misconfigured broker can't be hammered in a tight reconnect loop.
const MIN_AUTHENTICATION_BLACKOUT_MS = 1000

module.exports = class ConnectionPool {
  /**
   * @param {ConstructorParameters<typeof Connection>[0] & { authenticationTimeout?: number }} options
   */
  constructor(options) {
    this.logger = options.logger.namespace('ConnectionPool')
    this.connectionTimeout = options.connectionTimeout
    this.host = options.host
    this.port = options.port
    this.rack = options.rack
    this.ssl = options.ssl
    this.sasl = options.sasl
    this.clientId = options.clientId
    this.socketFactory = options.socketFactory
    this.authenticationTimeout = options.authenticationTimeout
    this.brokerAddress = `${this.host}:${this.port}`

    /** @type {Map<string, number>} broker address -> timestamp (ms) until which it is blacked out */
    this.authenticationBlackout = new Map()

    this.pool = new Array(2).fill().map(
      () =>
        new Connection({
          ...options,
          onAuthenticationFailure: () => this.recordAuthenticationFailure(),
        })
    )
  }

  /**
   * Called when a Connection reports SASL_AUTHENTICATION_FAILED (code 58). Blacks this
   * broker out for `max(authenticationTimeout, 1000ms)`, so getConnection() refuses to
   * reconnect immediately and turn a bad credential into a reconnect storm.
   * @public
   */
  recordAuthenticationFailure() {
    const blackoutMs = Math.max(this.authenticationTimeout || 0, MIN_AUTHENTICATION_BLACKOUT_MS)
    this.authenticationBlackout.set(this.brokerAddress, Date.now() + blackoutMs)
  }

  /**
   * @private
   * @returns {number|null} ms remaining in the blackout window, or null if not blacked out
   */
  authenticationBlackoutRemaining() {
    const until = this.authenticationBlackout.get(this.brokerAddress)
    if (until == null) {
      return null
    }

    const remaining = until - Date.now()
    if (remaining <= 0) {
      this.authenticationBlackout.delete(this.brokerAddress)
      return null
    }

    return remaining
  }

  isConnected() {
    return this.pool.some((c) => c.isConnected())
  }

  isAuthenticated() {
    return this.pool.some((c) => c.isAuthenticated())
  }

  setVersions(versions) {
    this.map((c) => c.setVersions(versions))
  }

  map(callback) {
    return this.pool.map((c) => callback(c))
  }

  async send(protocolRequest) {
    const connection = await this.getConnectionByRequest(protocolRequest)
    return connection.send(protocolRequest)
  }

  getConnectionByRequest({ request: { apiKey } }) {
    const index = { [apiKeys.Fetch]: 1 }[apiKey] || 0
    return this.getConnection(index)
  }

  async getConnection(index = 0) {
    const remainingBlackoutMs = this.authenticationBlackoutRemaining()
    if (remainingBlackoutMs != null) {
      throw new KafkaJSConnectionError(
        `SASL authentication recently failed for ${this.brokerAddress}; refusing to reconnect for another ${remainingBlackoutMs}ms`,
        { broker: this.brokerAddress }
      )
    }

    const connection = this.pool[index]

    if (!connection.isConnected()) {
      await connection.connect()
    }

    return connection
  }

  async destroy() {
    await Promise.all(this.map((c) => c.disconnect()))
  }
}
