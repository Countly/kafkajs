const { withAdminGuard } = require('../admin/deadline')
const apiKeys = require('../protocol/requests/apiKeys')
const Connection = require('./connection')
const { ConnectionSetupPolicy, NodeSetupStates, SetupAttempt } = require('./connectionSetup')

/** Fetch and share data use the second slot; coordinator requests use the first. @type {number} */
const POOL_SIZE = 2
const { KafkaJSConnectionError, KafkaJSSASLAuthenticationError } = require('../errors')

// Floor for the post-authentication-failure blackout, applied even when authenticationTimeout
// is unset or shorter, so a misconfigured broker can't be hammered in a tight reconnect loop.
const MIN_AUTHENTICATION_BLACKOUT_MS = 1000

/**
 * @param {(() => Error|null)|undefined} guard a producer's private pre-write check
 * @returns {void}
 * @throws {Error} the error the guard returned
 */
const throwIfRefused = (guard) => {
  const refusal = guard && guard()
  if (refusal) {
    throw refusal
  }
}

module.exports = class ConnectionPool {
  /**
   * @param {ConstructorParameters<typeof Connection>[0] & {
   *   authenticationTimeout?: number,
   *   setupGuard?: () => Error|null,
   *   authenticationBlackout?: Map<string, { until: number, errorMessage: string }>,
   *   drainTimeout?: number,
   *   generation?: number
   * }} options `drainTimeout` is how long `destroy()` waits for the requests of each connection
   *   (ms); unbounded when omitted. `generation` is the number the connections of the pool are
   *   counted on from (0 when omitted), see `continueGenerationFrom`.
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
    this.drainTimeout = options.drainTimeout
    this.retired = false
    this.setupGuard = options.setupGuard || null
    this.generationBase = options.generation ?? 0
    this.brokerAddress = `${this.host}:${this.port}`
    this.setupPolicy = options.setupPolicy || ConnectionSetupPolicy.resolve(options)
    this.setupStates = options.setupStates || new NodeSetupStates(this.setupPolicy)
    if (!options.setupStates) this.setupStates.retain([this.brokerAddress])

    // Broker address -> blackout entry. Defaults to a pool-local map, but a BrokerPool
    // rebuilding this pool after a connection error injects its own long-lived map instead
    // (see BrokerPool#buildConnectionPool), so the blackout survives the rebuild.
    this.authenticationBlackout = options.authenticationBlackout || new Map()

    this.pool = new Array(POOL_SIZE).fill().map(
      () =>
        new Connection({
          ...options,
          setupPolicy: this.setupPolicy,
          beginSetup: () => this.beginSetup(),
          onAuthenticationFailure: (errorMessage) => this.recordAuthenticationFailure(errorMessage),
        })
    )
  }

  /**
   * Called when a Connection reports SASL_AUTHENTICATION_FAILED (code 58). Blacks this
   * broker out for `max(authenticationTimeout, 1000ms)`, so getConnection() refuses to
   * (re)authenticate immediately and turn a bad credential into a reconnect storm.
   * @public
   * @param {string} errorMessage A sanitized description of the failure (mechanism, broker,
   *                              stage), surfaced again if a later attempt is refused during
   *                              the blackout; never the broker's own text.
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

  /** Creates one attempt against the current shared owner. @returns {SetupAttempt} */
  beginSetup() {
    this.throwIfRetired()
    const attempt = new SetupAttempt({
      states: this.setupStates,
      address: this.brokerAddress,
      policy: this.setupPolicy,
    })
    attempt.externalGuard = this.setupGuard
    return attempt
  }

  isConnected() {
    return this.pool.some((c) => c.isConnected())
  }

  /**
   * Marks this pool as replaced. From now on it opens no connection and sends nothing: a caller
   * that held a reference across an await gets a retriable connection error and looks the broker
   * up again. It stays retired; the connections it holds are closed by `destroy()`.
   * @public
   * @returns {void}
   */
  retire() {
    this.retired = true
    this.map((connection) => connection.cancelSetup())
  }

  /**
   * @private
   * @throws {KafkaJSConnectionError} when the pool was retired
   */
  throwIfRetired() {
    if (this.retired) {
      throw new KafkaJSConnectionError('This broker connection was replaced', {
        broker: this.brokerAddress,
      })
    }
  }

  /**
   * Changes whenever any socket in this pool is (re)established. Version information is only
   * valid for the connection it was learned on (KIP-35), so Broker keys its negotiated table on
   * this value rather than on one connection. It counts on from `generationBase`, which a
   * replacement pool inherits. Before its first connection it can equal the old baseline;
   * an established sending generation advances beyond that baseline.
   * @returns {number}
   */
  get generation() {
    let generation = this.generationBase
    for (let index = 0; index < this.pool.length; index += 1) {
      generation += this.pool[index].generation
    }
    return generation
  }

  /**
   * Reads the generation of the socket an API uses, without opening it. The pool baseline is
   * included, so an established generation is never reused after the pool is rebuilt. Read it after
   * the connection is established: reconnecting the other slot does not change it.
   * @param {number} apiKey Request API key; an unknown key is routed to the first slot.
   * @returns {number} The pool baseline plus the selected connection's generation.
   */
  generationFor(apiKey) {
    return this.generationBase + this.pool[this.connectionIndexFor(apiKey)].generation
  }

  /**
   * Selects the slot for routing and generation reads, from constants and without allocating.
   * @param {number} apiKey Request API key.
   * @returns {0|1} 1 for Fetch, ShareFetch and ShareAcknowledge; 0 for every other key.
   */
  connectionIndexFor(apiKey) {
    return POOL_SIZE > 1 &&
      (apiKey === apiKeys.Fetch ||
        apiKey === apiKeys.ShareFetch ||
        apiKey === apiKeys.ShareAcknowledge)
      ? 1
      : 0
  }

  /**
   * Counts the generation on from at least `generation`; never lowers it. A pool that replaces
   * another calls this when it is installed, so the numbers of the two pools never overlap
   * however far the replaced one advanced while the replacement was being built.
   * @public
   * @param {number} generation the generation of the pool being replaced
   * @returns {void}
   */
  continueGenerationFrom(generation) {
    this.generationBase = Math.max(this.generationBase, generation)
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
   * @param {object} [options]
   * @param {() => Error|null} [options.guard] a pre-write check, see `Connection#send`
   * @param {boolean} [established=false] private: the Broker already acquired the final connection
   *   and captured its generation. With a supplied connection, this path has no await before send
   *   and refuses a disconnected socket instead of reconnecting after that snapshot.
   * @returns {Promise} the broker's parsed response for `protocolRequest`
   * @throws {KafkaJSConnectionError} when the pool was retired
   */
  async send(protocolRequest, connection = null, options = null, established = false) {
    this.throwIfRetired()
    const resolvedConnection = connection || (await this.getConnectionByRequest(protocolRequest))
    this.throwIfRetired()

    if (!established && !resolvedConnection.isConnected()) {
      this.throwIfRetired()
      await resolvedConnection.connect()
      this.throwIfRetired()
    }

    this.throwIfRetired()
    if (established) {
      resolvedConnection.failIfNotConnected()
    }
    const guard = options?.guard
    return resolvedConnection.send(
      guard
        ? {
            ...protocolRequest,
            guard,
            responseGuard: withAdminGuard(protocolRequest.responseGuard),
          }
        : protocolRequest
    )
  }

  /**
   * @public
   * @param {{ request: { apiKey: number } }} protocolRequest
   * @param {{ guard?: () => Error|null }} [options] see `getConnection`
   * @returns {Promise<Connection>}
   * @throws {KafkaJSConnectionError} when the pool was retired
   */
  getConnectionByRequest({ request: { apiKey } }, options) {
    const index = this.connectionIndexFor(apiKey)
    return options ? this.getConnection(index, options) : this.getConnection(index)
  }

  /**
   * @public
   * @param {number} [index=0]
   * @param {object} [options]
   * @param {() => Error|null} [options.guard] a producer's private check of its lifecycle: asked
   *   immediately before the connection is opened and again once it is, an error it returns is
   *   thrown, so a closed producer opens no socket and goes on to no later stage
   * @returns {Promise<Connection>}
   * @throws {KafkaJSConnectionError} when the pool was retired
   * @throws {Error} the error the guard returned
   */
  async getConnection(index = 0, { guard } = {}) {
    this.throwIfRetired()
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
      throwIfRefused(guard)
      await connection.connect()
      throwIfRefused(guard)
    }

    this.throwIfRetired()
    return connection
  }

  /**
   * Closes every connection, each after its requests finished or `drainTimeout` passed. The pool
   * stays reusable unless it was retired.
   * @public
   * @param {object} [options]
   * @param {number} [options.drainTimeout] ms to wait for requests; the pool's own when omitted
   * @returns {Promise<void>}
   */
  async destroy({ drainTimeout = this.drainTimeout } = {}) {
    await Promise.all(this.map((c) => c.disconnect({ drainTimeout })))
  }
}
