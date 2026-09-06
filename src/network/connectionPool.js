const apiKeys = require('../protocol/requests/apiKeys')
const Connection = require('./connection')
const { KafkaJSSASLAuthenticationError } = require('../errors')

// Floor for the post-authentication-failure blackout, applied even when authenticationTimeout
// is unset or shorter, so a misconfigured broker can't be hammered in a tight reconnect loop.
const MIN_AUTHENTICATION_BLACKOUT_MS = 1000

module.exports = class ConnectionPool {
  /**
   * @param {ConstructorParameters<typeof Connection>[0] & {
   *   authenticationTimeout?: number,
   *   authenticationBlackout?: Map<string, { until: number, errorMessage: string }>
   * }} options
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

    // Broker address -> blackout entry. Defaults to a pool-local map, but a BrokerPool
    // rebuilding this pool after a connection error injects its own long-lived map instead
    // (see BrokerPool#buildConnectionPool), so the blackout survives the rebuild.
    this.authenticationBlackout = options.authenticationBlackout || new Map()

    this.pool = new Array(2).fill().map(
      () =>
        new Connection({
          ...options,
          onAuthenticationFailure: (errorMessage) => this.recordAuthenticationFailure(errorMessage),
        })
    )
  }

  /**
   * Called when a Connection reports SASL_AUTHENTICATION_FAILED (code 58). Blacks this
   * broker out for `max(authenticationTimeout, 1000ms)`, so getConnection() refuses to
   * (re)authenticate immediately and turn a bad credential into a reconnect storm.
   * @public
   * @param {string} errorMessage The broker's own error message, surfaced again if a later
   *                              attempt is refused during the blackout.
   */
  recordAuthenticationFailure(errorMessage) {
    const blackoutMs = Math.max(this.authenticationTimeout || 0, MIN_AUTHENTICATION_BLACKOUT_MS)
    this.authenticationBlackout.set(this.brokerAddress, {
      until: Date.now() + blackoutMs,
      errorMessage,
    })
  }

  /**
   * @private
   * @returns {{ remainingMs: number, errorMessage: string }|null} the active blackout entry
   *          for this broker, or null if it isn't blacked out (expired entries are pruned)
   */
  authenticationBlackoutEntry() {
    const entry = this.authenticationBlackout.get(this.brokerAddress)
    if (entry == null) {
      return null
    }

    const remainingMs = entry.until - Date.now()
    if (remainingMs <= 0) {
      this.authenticationBlackout.delete(this.brokerAddress)
      return null
    }

    return { remainingMs, errorMessage: entry.errorMessage }
  }

  isConnected() {
    return this.pool.some((c) => c.isConnected())
  }

  /**
   * Changes whenever any socket in this pool is (re)established. Version information is only
   * valid for the connection it was learned on (KIP-35), so Broker keys its negotiated table on
   * this value rather than on one connection.
   * @returns {number}
   */
  get generation() {
    return this.pool[0].generation + this.pool[1].generation
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

  /**
   * Sends one request on this pool.
   *
   * A caller-supplied connection is reconnected here if it died meanwhile - it may have been
   * resolved several awaits ago - so that this path behaves like `getConnection()`, which the
   * caller would otherwise have gone through.
   *
   * @param {object} protocolRequest a built request, as produced by a request builder
   * @param {Connection} [connection] the connection to send on, if the caller already resolved
   *                                  it (e.g. via `getConnectionByRequest`) for its own purposes;
   *                                  resolving it again here would be redundant, since it is
   *                                  always the same connection this request would resolve to
   * @returns {Promise} the broker's parsed response for `protocolRequest`
   */
  async send(protocolRequest, connection = null) {
    const resolvedConnection = connection || (await this.getConnectionByRequest(protocolRequest))

    if (!resolvedConnection.isConnected()) {
      await resolvedConnection.connect()
    }

    return resolvedConnection.send(protocolRequest)
  }

  getConnectionByRequest({ request: { apiKey } }) {
    const index = { [apiKeys.Fetch]: 1 }[apiKey] || 0
    return this.getConnection(index)
  }

  /**
   * @public
   * @param {number} [index=0]
   * @returns {Promise<Connection>}
   */
  async getConnection(index = 0) {
    const connection = this.pool[index]

    // The blackout only ever needs to gate a (re)authentication attempt: a connection that
    // is already connected and authenticated keeps working untouched, on this index and on
    // its sibling, even while another index is blacked out for this same broker.
    const needsAuthenticationAttempt =
      !connection.isConnected() || (this.sasl && !connection.isAuthenticated())
    if (needsAuthenticationAttempt) {
      const blackout = this.authenticationBlackoutEntry()
      if (blackout != null) {
        throw new KafkaJSSASLAuthenticationError(
          `SASL authentication recently failed for ${this.brokerAddress} (${blackout.errorMessage}); refusing to reconnect for another ${blackout.remainingMs}ms`
        )
      }
    }

    if (!connection.isConnected()) {
      await connection.connect()
    }

    return connection
  }

  async destroy() {
    await Promise.all(this.map((c) => c.disconnect()))
  }
}
