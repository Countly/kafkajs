const { isNativeError } = require('util').types
const describeThrown = require('./describeThrown')
const createSocket = require('./socket')
const createRequest = require('../protocol/request')
const Decoder = require('../protocol/decoder')
const { NETWORK_AUTHENTICATION_FAILURE } = require('./instrumentationEvents')
const {
  KafkaJSConnectionError,
  KafkaJSNonRetriableError,
  KafkaJSConnectionClosedError,
  KafkaJSInvalidFrameError,
} = require('../errors')
const { INT_32_MAX_VALUE, DEFAULT_MAX_RESPONSE_SIZE } = require('../constants')
const FrameBuffer = require('./frameBuffer')
const getEnv = require('../env')
const RequestQueue = require('./requestQueue')
const { CONNECTION_STATUS, CONNECTED_STATUS } = require('./connectionStatus')
const sharedPromiseTo = require('../utils/sharedPromiseTo')
const Long = require('../utils/long')
const { ConnectionSetupPolicy, SetupAttempt } = require('./connectionSetup')
const SASLAuthenticator = require('../broker/saslAuthenticator')
const apiKeys = require('../protocol/requests/apiKeys')
const { responseHeaderVersion, RESPONSE_HEADER_V1 } = require('../protocol/headers')

const requestInfo = ({ apiName, apiKey, apiVersion }) =>
  `${apiName}(key: ${apiKey}, version: ${apiVersion})`

/**
 * The response header of a flexible version carries a tagged-field buffer after the
 * correlation id. `processData` only knows the correlation id, so the buffer is consumed here,
 * where the request entry says which `(apiKey, apiVersion)` produced this payload. ApiVersions
 * is the standing exception: its response header is always v0.
 * @see https://kafka.apache.org/43/design/protocol
 * @param {{ apiKey: number, apiVersion: number }} entry
 * @param {Buffer} payload the response bytes after the correlation id
 * @returns {Buffer} the body the codec should decode
 */
const responseBody = ({ apiKey, apiVersion }, payload) => {
  if (responseHeaderVersion(apiKey, apiVersion) !== RESPONSE_HEADER_V1) {
    return payload
  }

  // Fast path: no broker sends a non-empty response header tag buffer today, and this runs on
  // every response, so the common case skips allocating a Decoder and a Map to consume one byte.
  if (payload[0] === 0) {
    return payload.slice(1)
  }

  const decoder = new Decoder(payload)
  decoder.readTaggedFields()
  return payload.slice(decoder.offset)
}

/**
 * @param request - request from protocol
 * @returns {boolean}
 */
const isAuthenticatedRequest = (request) => {
  return ![apiKeys.ApiVersions, apiKeys.SaslHandshake, apiKeys.SaslAuthenticate].includes(
    request.apiKey
  )
}

const PRIVATE = {
  SHOULD_REAUTHENTICATE: Symbol('private:Connection:shouldReauthenticate'),
  AUTHENTICATE: Symbol('private:Connection:authenticate'),
  PREPARE: Symbol('private:Connection:prepare'),
  ENQUEUE: Symbol('private:Connection:enqueue'),
  QUEUED: Symbol('private:Connection:queued'),
  CONNECT: Symbol('private:Connection:connect'),
  CONNECT_ATTEMPT: Symbol('private:Connection:connectAttempt'),
  ABORT_CONNECT: Symbol('private:Connection:abortConnect'),
}

module.exports = class Connection {
  /**
   * @param {Object} options
   * @param {string} options.host
   * @param {number} options.port
   * @param {import("../../types").Logger} options.logger
   * @param {import("../../types").ISocketFactory} options.socketFactory
   * @param {string} [options.clientId='kafkajs']
   * @param {number} options.requestTimeout The maximum amount of time the client will wait for the response of a request,
   *                                in milliseconds
   * @param {string} [options.rack=null]
   * @param {Object} [options.ssl=null] Options for the TLS Secure Context. It accepts all options,
   *                            usually "cert", "key" and "ca". More information at
   *                            https://nodejs.org/api/tls.html#tls_tls_createsecurecontext_options
   * @param {Object} [options.sasl=null] Attributes used for SASL authentication. Options based on the
   *                             key "mechanism". Connection is not actively using the SASL attributes
   *                             but acting as a data object for this information
   * @param {number} [options.reauthenticationThreshold=10000]
   * @param {number} options.connectionTimeout Resolved base in milliseconds
   * @param {number} [options.connectionTimeoutMax] Resolved setup ceiling
   * @param {number} [options.reconnectBackoff] Resolved retry wait base
   * @param {number} [options.reconnectBackoffMax] Resolved retry wait ceiling
   * @param {number} [options.authenticationTimeout] Whole-SASL cap in milliseconds
   * @param {ConnectionSetupPolicy} [options.setupPolicy] Policy resolved before downstream defaults
   * @param {() => SetupAttempt} [options.beginSetup] Current shared node owner callback
   * @param {boolean} [options.enforceRequestTimeout]
   * @param {number} [options.maxResponseSize=104857600] The largest response body accepted, in bytes,
   *                                            the 4-byte length prefix excluded. A larger, negative or
   *                                            impossibly short frame closes the connection with
   *                                            `KafkaJSInvalidFrameError`.
   * @param {number|null} [options.maxInFlightRequests=5] The maximum number of unacknowledged requests on a
   *                                            connection before enqueuing (Java's
   *                                            max.in.flight.requests.per.connection default).
   *                                            Pass null for unbounded (the pre-3.0 behavior).
   * @param {import("../instrumentation/emitter")} [options.instrumentationEmitter=null]
   * @param {(errorMessage: string) => void} [options.onAuthenticationFailure=null] Called with the
   *                                            broker's error message when it rejects
   *                                            SaslAuthenticate (code 58), after the
   *                                            NETWORK_AUTHENTICATION_FAILURE event is emitted.
   *                                            Used by ConnectionPool to blackout the broker.
   */
  constructor({
    host,
    port,
    logger,
    socketFactory,
    requestTimeout,
    reauthenticationThreshold = 10000,
    rack = null,
    ssl = null,
    sasl = null,
    clientId = 'kafkajs',
    connectionTimeout,
    authenticationTimeout,
    connectionTimeoutMax,
    reconnectBackoff,
    reconnectBackoffMax,
    setupPolicy,
    beginSetup,
    enforceRequestTimeout = true,
    maxResponseSize = DEFAULT_MAX_RESPONSE_SIZE,
    maxInFlightRequests = 5,
    instrumentationEmitter = null,
    onAuthenticationFailure = null,
  }) {
    this.host = host
    this.port = port
    this.rack = rack
    this.clientId = clientId
    this.broker = `${this.host}:${this.port}`
    this.logger = logger.namespace('Connection')

    this.socketFactory = socketFactory
    this.ssl = ssl
    this.sasl = sasl

    this.requestTimeout = requestTimeout
    this.connectionTimeout = connectionTimeout
    this.setupPolicy =
      setupPolicy ||
      ConnectionSetupPolicy.resolve({
        connectionTimeout,
        connectionTimeoutMax,
        reconnectBackoff,
        reconnectBackoffMax,
        authenticationTimeout,
      })
    this.connectionTimeout = this.setupPolicy.timeoutBase
    this.connectionTimeoutMax = connectionTimeoutMax ?? this.setupPolicy.timeoutCeiling
    this.reconnectBackoff = reconnectBackoff ?? this.setupPolicy.waitBase
    this.reconnectBackoffMax = reconnectBackoffMax ?? this.setupPolicy.waitCeiling
    this.authenticationTimeout = this.setupPolicy.authenticationTimeout
    this.beginSetup =
      beginSetup || (() => new SetupAttempt({ address: this.broker, policy: this.setupPolicy }))
    this.setupAttempt = null
    this.authEpisode = null
    this.reauthenticationThreshold = reauthenticationThreshold
    this.instrumentationEmitter = instrumentationEmitter
    this.onAuthenticationFailure = onAuthenticationFailure

    /**
     * Splits what the socket delivers into response frames; reset on every connect and every close.
     * @type {FrameBuffer}
     */
    this.frames = new FrameBuffer({
      maxBodySize: maxResponseSize,
      broker: this.broker,
      onFrame: (body) => this.fulfillFrame(body),
      minBodyOf: (correlationId) => this.minBodyOf(correlationId),
    })

    this.connectionStatus = CONNECTION_STATUS.DISCONNECTED
    this.correlationId = 0

    /**
     * Bumped every time a socket is established. Version information learned over one socket is
     * only valid for that socket (KIP-35), so callers key their caches on this.
     * @type {number}
     */
    this.generation = 0

    /**
     * Identifies the current `connect()` attempt (0 = none yet). Taken before the socket for
     * that attempt is created — not after, and not derived from `this.socket` — so an attempt
     * is recognised as current regardless of when its `socketFactory` calls `onConnect`, and so
     * that `disconnect()` can invalidate a superseded attempt by bumping this counter alone.
     * @type {number}
     */
    this.connectAttempt = 0

    /**
     * Rejects the in-flight `connect()` attempt, if any; set for the lifetime of one attempt and
     * cleared once it settles. `disconnect()` calls it so a caller blocked on `connect()` is
     * rejected immediately when the attempt is superseded, rather than left to its own timeout.
     * @type {((error: Error) => void)|null}
     */
    this[PRIVATE.ABORT_CONNECT] = null

    this.requestQueue = new RequestQueue({
      instrumentationEmitter,
      maxInFlightRequests,
      requestTimeout,
      enforceRequestTimeout,
      clientId,
      broker: this.broker,
      logger: logger.namespace('RequestQueue'),
      isConnected: () => this.isConnected(),
      onRequestTimeout: () => {
        const error = new KafkaJSConnectionClosedError('Closed connection', {
          host: this.host,
          port: this.port,
        })
        this.setupAttempt?.fail(error)
        this.closeGeneration(error)
      },
    })

    this.versions = null

    const log =
      (level) =>
      (message, extra = {}) => {
        const logFn = this.logger[level]
        logFn(message, { broker: this.broker, clientId, ...extra })
      }

    this.logDebug = log('debug')
    this.logError = log('error')

    const env = getEnv()
    this.shouldLogBuffers = env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS === '1'
    this.shouldLogFetchBuffer =
      this.shouldLogBuffers && env.KAFKAJS_DEBUG_EXTENDED_PROTOCOL_BUFFERS === '1'

    this.authenticatedAt = null
    this.sessionLifetime = Long.ZERO

    /**
     * Settles once the latest request of the current attempt has reached the request queue,
     * whatever its outcome. Each `connect()` attempt starts a fresh one, so a request of an
     * ended attempt never holds up the next.
     */
    this[PRIVATE.QUEUED] = Promise.resolve()

    /**
     * @private
     * @returns {Promise}
     */
    this[PRIVATE.AUTHENTICATE] = () => this.authenticate()

    this[PRIVATE.CONNECT] = sharedPromiseTo(async (supplied) => {
      if (!supplied && this.connectionStatus === CONNECTION_STATUS.CONNECTED) return true
      const attempt = supplied || this.beginSetup()
      this.setupAttempt = attempt
      try {
        if (attempt.waitMs() > 0) await attempt.wait()
        else attempt.assertCurrent()
        attempt.arm((error) => {
          if (this.setupAttempt === attempt) this.failGeneration(error)
        })
        await attempt.guard(this[PRIVATE.CONNECT_ATTEMPT](), {
          phase: 'TCP/TLS',
        })
        if (!supplied && !attempt.brokerManaged) {
          this.finishSetup(attempt)
        }
        return true
      } catch (error) {
        attempt.fail(error)
        throw error
      }
    })
  }

  setVersions(versions) {
    this.versions = versions
  }

  isConnected() {
    return CONNECTED_STATUS.includes(this.connectionStatus)
  }

  /**
   * Connects this connection's socket, negotiating nothing itself. Concurrent callers — the
   * request path calls this whenever a socket looks disconnected, with no lock — share one
   * underlying attempt and settle together, so at most one socket is ever being created at a
   * time for this connection.
   *
   * @public
   * @param {{ attempt?: SetupAttempt }} [options] externally managed broker readiness budget
   * @returns {Promise<boolean>} socket established within its original budget
   * @throws {KafkaJSConnectionError} on cancellation, expiry or transport failure
   */
  connect({ attempt } = {}) {
    return this[PRIVATE.CONNECT](attempt)
  }

  /**
   * The actual connect attempt, wrapped by `connect()` in `sharedPromiseTo` so overlapping
   * callers share it. Every socket event handler below is tied to this attempt's token
   * (`this.connectAttempt`), taken *before* `createSocket` runs so a `socketFactory` that
   * invokes `onConnect` synchronously — a documented extension point — is still recognised as
   * current; comparing against `this.socket` instead broke exactly that case. A later attempt,
   * or a `disconnect()` call, bumps `this.connectAttempt` and turns every handler here into a
   * no-op for the attempt it belonged to.
   *
   * @private
   * @returns {Promise}
   */
  [PRIVATE.CONNECT_ATTEMPT]() {
    return new Promise((resolve, reject) => {
      // CONNECTED_STATUS treats DISCONNECTING as "connected" so requests already in flight can
      // still complete while draining — right for send()/RequestQueue, wrong here: resolving
      // `true` while a disconnect() is in progress would hand back a connection that is seconds
      // (or a tick) from being DISCONNECTED, with no new socket ever created for this caller.
      if (this.connectionStatus === CONNECTION_STATUS.DISCONNECTING) {
        return reject(
          new KafkaJSConnectionError('Connection is disconnecting', {
            broker: `${this.host}:${this.port}`,
          })
        )
      }

      if (this.isConnected()) {
        return resolve(true)
      }

      this.authenticatedAt = null

      const attempt = ++this.connectAttempt
      this[PRIVATE.QUEUED] = Promise.resolve()
      const isCurrentAttempt = () => this.connectAttempt === attempt

      const abort = (error) => {
        this[PRIVATE.ABORT_CONNECT] = null
        reject(error)
      }
      this[PRIVATE.ABORT_CONNECT] = abort

      const onConnect = () => {
        if (!isCurrentAttempt()) return
        this[PRIVATE.ABORT_CONNECT] = null
        this.frames.reset()
        this.connectionStatus = CONNECTION_STATUS.CONNECTED
        this.requestQueue.scheduleRequestTimeoutCheck()
        this.generation += 1
        resolve(true)
      }

      const onData = (data) => {
        if (!isCurrentAttempt()) return
        try {
          this.processData(data)
        } catch (error) {
          let framed = false
          try {
            framed = isNativeError(error) && error instanceof KafkaJSInvalidFrameError
          } catch {}
          this.failGeneration(
            framed
              ? error
              : new KafkaJSInvalidFrameError('Unreadable response frame', {
                  broker: this.broker,
                  reason: 'unreadable',
                  cause: error,
                }),
            framed ? null : 'Unreadable response frame: '
          )
        }
      }

      const onEnd = () => {
        if (!isCurrentAttempt()) return
        const error = this.isConnected()
          ? new KafkaJSConnectionClosedError('Closed connection', {
              host: this.host,
              port: this.port,
            })
          : new KafkaJSConnectionError('Connection closed while connecting', {
              broker: this.broker,
            })
        this.failGeneration(error)
      }

      const onError = (e) => {
        if (!isCurrentAttempt()) return
        const error = new KafkaJSConnectionError(`Connection error: ${e.message}`, {
          broker: this.broker,
          code: e.code,
        })
        this.failGeneration(error)
        try {
          this.logError(error.message, { stack: e.stack })
        } catch {}
      }

      const onTimeout = () => {
        if (!isCurrentAttempt()) return
        const error = new KafkaJSConnectionError('Connection timeout', {
          broker: this.broker,
        })
        this.failGeneration(error)
        try {
          this.logError(error.message)
        } catch {}
      }

      try {
        this.logDebug('Connecting', { ssl: !!this.ssl, sasl: !!this.sasl })
      } catch (error) {
        const native = isNativeError(error)
        this.failGeneration(
          native
            ? error
            : new KafkaJSNonRetriableError('Failed to connect', {
                cause: error,
              }),
          native ? null : 'Failed to connect: '
        )
        return
      }

      try {
        const socket = createSocket({
          socketFactory: this.socketFactory,
          host: this.host,
          port: this.port,
          ssl: this.ssl,
          onConnect,
          onData,
          onEnd,
          onError,
          onTimeout,
        })
        if (!isCurrentAttempt()) {
          socket.destroy()
        } else {
          this.socket = socket
        }
      } catch (e) {
        this.failGeneration(
          new KafkaJSConnectionError('Failed to connect', {
            broker: this.broker,
            cause: e,
          }),
          'Failed to connect: '
        )
      }
    })
  }

  /**
   * Closes this connection: waits for the pending and in-flight requests, for at most
   * `drainTimeout` when given, then ends the generation with the teardown `failGeneration` shares.
   * It rejects any in-flight `connect()` attempt it supersedes (KIP-35 rule 7 depends on a
   * reconnect never leaving a caller hanging past its timeout) with a plain, retriable
   * `KafkaJSConnectionError` — not `KafkaJSConnectionClosedError`, which several `src/cluster/`
   * branches do not recognise by name, since it would otherwise skip seed-broker rotation and
   * metadata refresh on a connect that was merely superseded, not actually refused.
   *
   * The socket to tear down and the epoch to end are captured up front, before the drain yields: a
   * concurrent `disconnect()`/`connect()` pair can otherwise install a new socket while this call is
   * still draining, and its continuation must act on the socket and epoch it owns. A drain that
   * outlives `drainTimeout` ends with the requests still pending rejected with
   * `KafkaJSConnectionClosedError`; `drainTimeout: 0` closes at once. The socket is destroyed, not
   * ended, so a half-open one cannot linger. The connection stays reusable: the next `connect()`
   * opens a new socket.
   *
   * @public
   * @param {object} [options]
   * @param {number} [options.drainTimeout] ms to wait for requests to finish; unbounded when omitted
   * @returns {Promise<true>}
   */
  async disconnect({ drainTimeout } = {}) {
    this.cancelSetup(
      new KafkaJSConnectionError('Connection closed while connecting', { broker: this.broker })
    )
    this.authenticatedAt = null
    this.connectionStatus = CONNECTION_STATUS.DISCONNECTING
    const abortConnect = this[PRIVATE.ABORT_CONNECT]
    this[PRIVATE.ABORT_CONNECT] = null
    if (abortConnect) {
      abortConnect(
        new KafkaJSConnectionError('Connection closed while connecting', {
          broker: `${this.host}:${this.port}`,
        })
      )
    }
    const socket = this.socket
    const epoch = this.connectAttempt

    // In-flight requests still need this socket's data handler, so the drain runs on the
    // current epoch; the epoch ends only afterwards.
    let drainFailed = false
    let drainFailure
    try {
      await this.drain(drainTimeout)
    } catch (error) {
      drainFailed = true
      drainFailure = error
    }

    // A newer connect()/disconnect() pair that ran while the drain was yielding already owns
    // this connection's socket and status; this disconnect() has nothing left to do.
    if (this.connectAttempt !== epoch) {
      return true
    }

    this.closeGeneration(
      new KafkaJSConnectionClosedError('Closed connection', {
        host: this.host,
        port: this.port,
      }),
      socket || this.socket
    )
    if (drainFailed) {
      try {
        this.logError('Failed to drain connection', {
          error: describeThrown(drainFailure),
          cause: drainFailure,
        })
      } catch {}
    }
    this.requestQueue.emitQueueState()
    try {
      this.logDebug('disconnected')
    } catch {}
    return true
  }

  /**
   * Ends the current generation at once, synchronously and idempotently: a call on a connection
   * with no live generation does nothing. The order matters, so that no observer and no promise
   * reaction can see a half-failed connection: the attempt ends and the connection is marked
   * disconnected, the socket is destroyed, the framing state is reset, every other in-flight and
   * pending request is rejected with `error`, and only then do the queue's observers run, each
   * contained. A request still encoding is rejected when its encoder returns, and a late reply on
   * the destroyed socket is dropped. The next request reconnects.
   *
   * @public
   * @param {Error} error what every request left on this connection is rejected with
   * @param {string|null} [messagePrefix=null] describe the original cause after close and settlement,
   *   before observers run; only freshly constructed wrappers supply a prefix
   * @returns {void}
   */
  failGeneration(error, messagePrefix = null) {
    this.setupAttempt?.fail(error)
    if (
      !this.socket &&
      !this[PRIVATE.ABORT_CONNECT] &&
      this.connectionStatus === CONNECTION_STATUS.DISCONNECTED
    ) {
      return
    }

    this.closeGeneration(error)
    if (messagePrefix !== null) {
      error.message = messagePrefix + describeThrown(error.cause)
    }
    this.requestQueue.emitQueueState()
    try {
      this.logDebug('Generation failed', {
        cause: describeThrown(error, 'name'),
        ...this.frames.usage,
      })
    } catch {}
  }

  /**
   * The teardown `failGeneration` and `disconnect()` share, with no observer run.
   * @private
   * @param {Error} error what the requests left on the connection are rejected with
   * @param {object} [socket=this.socket] the socket of the generation being ended
   * @returns {void}
   */
  closeGeneration(error, socket = this.socket) {
    this.cancelSetup(error)
    const abortConnect = this[PRIVATE.ABORT_CONNECT]
    this[PRIVATE.ABORT_CONNECT] = null
    this.connectAttempt += 1
    this.authenticatedAt = null
    this.connectionStatus = CONNECTION_STATUS.DISCONNECTED
    if (abortConnect) {
      abortConnect(error)
    }

    // `error` stays attached, inert once the attempt is over: a post-close error on the old socket
    // (e.g. ECONNRESET) must not become an uncaught exception.
    if (socket && !socket.destroyed) {
      if (typeof socket.removeAllListeners === 'function') {
        socket.removeAllListeners('data')
        socket.removeAllListeners('end')
        socket.removeAllListeners('timeout')
      }
      socket.destroy()
    }

    if (this.socket === socket) {
      this.socket = null
    }

    this.frames.reset()
    this.requestQueue.settleAll(error)
    this.requestQueue.destroy()
  }

  /**
   * Waits for the requests of this connection to finish, for at most `drainTimeout` when given.
   * The one timer is cleared whichever way the wait ends.
   * @private
   * @param {number} [drainTimeout] ms; unbounded when omitted, none when 0
   * @returns {Promise<void>}
   */
  async drain(drainTimeout) {
    if (drainTimeout === undefined) {
      return this.requestQueue.waitForPendingRequests()
    }

    if (drainTimeout === 0) {
      return
    }

    let timer
    const bound = new Promise((resolve) => {
      timer = setTimeout(resolve, drainTimeout)
    })

    try {
      await Promise.race([this.requestQueue.waitForPendingRequests(), bound])
    } finally {
      clearTimeout(timer)
    }
  }

  /**
   * @public
   * @returns {boolean}
   */
  isAuthenticated() {
    return this.authenticatedAt != null && !this[PRIVATE.SHOULD_REAUTHENTICATE]()
  }

  /***
   * @private
   */
  [PRIVATE.SHOULD_REAUTHENTICATE]() {
    if (this.sessionLifetime.equals(Long.ZERO)) {
      return false
    }

    const [secondsSince, remainingNanosSince] = process.hrtime(this.authenticatedAt)
    const millisSince = Long.fromValue(secondsSince)
      .multiply(1000)
      .add(Long.fromValue(remainingNanosSince).divide(1000000))

    const reauthenticateAt = millisSince.add(this.reauthenticationThreshold)
    return reauthenticateAt.greaterThanOrEqual(this.sessionLifetime)
  }

  /** Completes readiness once; idle tracking extends this single boundary.
   * @param {SetupAttempt} attempt original ready attempt
   * @returns {void}
   * @throws {KafkaJSConnectionError} if the original budget ended
   */
  finishSetup(attempt) {
    attempt.ready()
    if (this.setupAttempt === attempt) this.setupAttempt = null
  }

  /**
   * Cancels setup and detaches any shared authentication episode before teardown awaits.
   * @param {Error} [error] close reason
   * @returns {void}
   */
  cancelSetup(error) {
    this.setupAttempt?.cancel(error)
    this.setupAttempt = null
    this.authEpisode?.budget.cancel(error)
    this.authEpisode = null
  }

  /**
   * Shares one bounded SASL episode on this socket; never adopts a replacement token.
   * @param {{ attempt?: SetupAttempt }} [options] Broker setup budget when applicable
   * @returns {Promise<void>}
   * @throws {KafkaJSConnectionError} when the whole SASL phase expires
   */
  authenticate({ attempt = this.setupAttempt } = {}) {
    if (!this.sasl || this.isAuthenticated()) return Promise.resolve()
    if (this.authEpisode) return this.authEpisode.promise
    const epoch = this.connectAttempt
    const socket = this.socket
    const budget =
      attempt ||
      new SetupAttempt({
        address: this.broker,
        policy: this.setupPolicy,
        timeoutMs: this.authenticationTimeout,
      })
    const guard = () =>
      budget.refusal() ||
      (this.connectAttempt !== epoch || this.socket !== socket
        ? new KafkaJSConnectionError('Authentication connection was replaced', {
            broker: this.broker,
          })
        : null)
    const episode = { budget, promise: null }
    this.authEpisode = episode
    if (!attempt)
      budget.arm((error) => {
        if (this.connectAttempt === epoch && this.socket === socket) this.failGeneration(error)
      })
    episode.promise = (async () => {
      try {
        const authenticator = new SASLAuthenticator(this, this.logger, this.versions, guard)
        await budget.guard(authenticator.authenticate(), {
          phase: 'SASL',
          cap: this.authenticationTimeout,
        })
        const refusal = guard()
        if (refusal) throw refusal
        this.authenticatedAt = process.hrtime()
        this.sessionLifetime = Long.fromValue(authenticator.sessionLifetime)
        if (!attempt) budget.ready()
      } catch (error) {
        budget.fail(error)
        throw error
      } finally {
        if (this.authEpisode === episode) this.authEpisode = null
      }
    })()
    return episode.promise
  }

  /**
   * @public
   * Reports a SASL authentication failure diagnosed from the broker's own response
   * (SASL_AUTHENTICATION_FAILED, code 58): emits NETWORK_AUTHENTICATION_FAILURE and lets
   * the owning ConnectionPool blackout this broker before the next connection attempt.
   * @param {string} mechanism
   * @param {string} errorMessage
   */
  reportAuthenticationFailure(mechanism, errorMessage) {
    if (this.instrumentationEmitter) {
      this.instrumentationEmitter.emit(NETWORK_AUTHENTICATION_FAILURE, {
        broker: this.broker,
        mechanism,
        errorMessage,
      })
    }

    if (this.onAuthenticationFailure) {
      this.onAuthenticationFailure(errorMessage)
    }
  }

  /**
   * Sends one request and resolves with its parsed response.
   *
   * Requests reach this connection's request queue - and so the socket - in the order `send()` was
   * called. Each one authenticates (when due) and encodes at once, concurrently with the others,
   * and is pushed into the queue only after the request before it was, so a request that takes
   * longer to encode is never overtaken by a later one. The idempotent producer relies on it, since
   * the batches of one partition must reach the broker in the order they were numbered. The SASL
   * handshake and the ApiVersions exchange that authentication itself sends are not ordered, or the
   * first request would wait for itself.
   *
   * The order belongs to one connection attempt. A request still encoding when `disconnect()` or
   * a closed socket ends its attempt is rejected with a `KafkaJSConnectionError`, once its encoder
   * returns, and is never written - not even to the socket of a later attempt - and it never
   * delays a request sent after the next `connect()`.
   * @public
   * @param {object} protocol
   * @param {object} protocol.request It is defined by the protocol and consists of an object with "apiKey",
   *                         "apiVersion", "apiName" and an "encode" function. The encode function
   *                         must return an instance of Encoder
   *
   * @param {object} protocol.response It is defined by the protocol and consists of an object with two functions:
   *                          "decode" and "parse"
   *
   * @param {number} [protocol.requestTimeout=null] Override for the default requestTimeout
   * @param {boolean} [protocol.logResponseError=true] Whether to log errors
   * @param {(frameLength?: number) => Error|null} [protocol.guard=null] A pre-write check. It runs once the request is
   *                         encoded and again at the instant of transmission; an error it returns
   *                         rejects this one request, which is then written nowhere, takes no
   *                         in-flight slot and leaves the connection open.
   * @param {(() => Error|null)|null} [protocol.responseGuard=null] original setup/authentication
   *                         guard, checked after decode and parse before publishing side effects
   * @returns {Promise<data>} where data is the return of "response#parse"
   */
  async send({
    request,
    response,
    requestTimeout = null,
    logResponseError = true,
    guard = null,
    responseGuard = null,
  }) {
    const attempt = this.connectAttempt
    const expectResponse = !request.expectResponse || request.expectResponse()
    const prepared = this[PRIVATE.PREPARE]({ request, expectResponse, guard })
    const enqueue = (encoded) => this[PRIVATE.ENQUEUE](encoded, requestTimeout, attempt, guard)

    let queued
    if (isAuthenticatedRequest(request)) {
      // It may reject before the chain reaches it; marked handled so Node does not report it.
      prepared.catch(() => {})
      queued = this[PRIVATE.QUEUED].then(() => prepared).then(enqueue)
      this[PRIVATE.QUEUED] = queued.then(
        () => {},
        () => {}
      )
    } else {
      queued = prepared.then(enqueue)
    }

    const { answered } = await queued
    const { correlationId, size, entry, payload } = await answered

    if (!expectResponse) {
      return
    }

    try {
      const payloadDecoded = await response.decode(responseBody(entry, payload))
      const decodedRefusal = responseGuard && responseGuard()
      if (decodedRefusal) throw decodedRefusal

      /**
       * @see KIP-219
       * If the response indicates that the client-side needs to throttle, do that.
       */
      this.requestQueue.maybeThrottle(payloadDecoded.clientSideThrottleTime)

      const data = await response.parse(payloadDecoded)
      const parsedRefusal = responseGuard && responseGuard()
      if (parsedRefusal) throw parsedRefusal
      const isFetchApi = entry.apiName === 'Fetch'
      this.logDebug(`Response ${requestInfo(entry)}`, {
        correlationId,
        size,
        data: isFetchApi && !this.shouldLogFetchBuffer ? '[filtered]' : data,
      })

      return data
    } catch (e) {
      if (logResponseError) {
        this.logError(`Response ${requestInfo(entry)}`, {
          error: e.message,
          correlationId,
          size,
        })
      }

      const isBuffer = Buffer.isBuffer(payload)
      this.logDebug(`Response ${requestInfo(entry)}`, {
        error: e.message,
        correlationId,
        payload:
          isBuffer && !this.shouldLogBuffers ? { type: 'Buffer', data: '[filtered]' } : payload,
      })

      throw e
    }
  }

  /**
   * Authenticates when needed and encodes the request under the next correlation id.
   * @private
   * @param {{ request: object, expectResponse: boolean, guard: ((frameLength?: number) => Error|null)|null }} options
   * @returns {Promise<{ request: object, expectResponse: boolean, correlationId: number,
   *   requestPayload: object }>}
   * @throws {Error} the error the guard returns, once the request is authenticated and encoded
   */
  async [PRIVATE.PREPARE]({ request, expectResponse, guard }) {
    if (!this.isAuthenticated() && isAuthenticatedRequest(request)) {
      await this[PRIVATE.AUTHENTICATE]()
    }

    this.failIfNotConnected()

    const { clientId } = this
    const correlationId = this.nextCorrelationId()
    const requestPayload = await createRequest({
      request,
      correlationId,
      clientId,
    })
    this.logDebug(`Request ${requestInfo(request)}`, {
      correlationId,
      expectResponse,
      size: Buffer.byteLength(requestPayload.buffer),
    })

    const refusal = guard && guard()
    if (refusal) {
      throw refusal
    }

    return { request, expectResponse, correlationId, requestPayload }
  }

  /**
   * Pushes an encoded request into the request queue.
   * @private
   * @param {{ request: object, expectResponse: boolean, correlationId: number,
   *   requestPayload: object }} encoded what `PREPARE` resolved with
   * @param {number|null} requestTimeout
   * @param {number} attempt the `connectAttempt` the request was sent under; a request of an older
   *   attempt is refused
   * @param {((frameLength?: number) => Error|null)|null} guard checked again by the queue at transmission
   * @returns {{ answered: Promise<{ correlationId: number, size: number, entry: object,
   *   payload: Buffer }> }} `answered` settles with the response frame; wrapped so that waiting
   *   for the push does not wait for the response
   */
  [PRIVATE.ENQUEUE](
    { request, expectResponse, correlationId, requestPayload },
    requestTimeout,
    attempt,
    guard
  ) {
    const { apiKey, apiName, apiVersion } = request
    const answered = new Promise((resolve, reject) => {
      try {
        this.failIfNotConnected(attempt)
        const entry = {
          apiKey,
          apiName,
          apiVersion,
          correlationId,
          resolve,
          reject,
        }

        this.requestQueue.push({
          entry,
          expectResponse,
          requestTimeout,
          guard,
          frameLength: requestPayload.buffer.length,
          sendRequest: () => {
            this.socket.write(requestPayload.buffer, 'binary')
          },
        })
      } catch (e) {
        reject(e)
      }
    })

    return { answered }
  }

  /**
   * Gates a *new* send, not an already in-flight one: deliberately stricter than `isConnected()`,
   * which also counts `DISCONNECTING` so `ConnectionPool` doesn't race a reconnect against an
   * orderly teardown. `disconnect()` sets `DISCONNECTING` synchronously, before its first
   * `await`, so this check rejects a `send()` call arriving during the drain immediately, at the
   * earliest possible point — before `socket.write()` or `requestQueue.push()` ever run. Letting
   * such a send() through used to be able to enqueue a request in the gap between the queue
   * reporting empty and `disconnect()`'s continuation destroying it and detaching the socket's
   * listeners, leaving that request with no response path, no close path and no timeout timer:
   * it would never settle. A request already in flight when `disconnect()` starts is unaffected —
   * it settles via `processData`, `rejectRequests`, or the timeout interval, none of which route
   * through this check.
   *
   * @private
   * @param {number} [attempt] the `connectAttempt` a request was sent under; it is refused once
   *   the connection has moved on to another attempt, even if it is connected again
   * @throws {KafkaJSConnectionError} when the connection is not connected, or `attempt` is over
   */
  failIfNotConnected(attempt = this.connectAttempt) {
    if (this.connectionStatus !== CONNECTION_STATUS.CONNECTED || attempt !== this.connectAttempt) {
      throw new KafkaJSConnectionError('Not connected', {
        broker: `${this.host}:${this.port}`,
      })
    }
  }

  /**
   * @private
   */
  nextCorrelationId() {
    if (this.correlationId >= INT_32_MAX_VALUE) {
      this.correlationId = 0
    }

    return this.correlationId++
  }

  /**
   * Feeds what the socket delivered to the frame buffer, which hands every complete frame to
   * `fulfillFrame` as it is found.
   * @private
   * @param {Buffer} rawData
   * @throws {KafkaJSInvalidFrameError} for a frame that is negative, too short or above the limit
   */
  processData(rawData) {
    this.frames.push(rawData)
  }

  /**
   * Hands a response frame to the request queue: the correlation id first, then the rest.
   * @private
   * @param {Buffer} body
   */
  fulfillFrame(body) {
    this.requestQueue.fulfillRequest({
      size: body.length,
      correlationId: body.readInt32BE(0),
      payload: body.subarray(Decoder.int32Size()),
    })
  }

  /**
   * @private
   * @param {number} correlationId
   * @returns {number} the smallest body the response to that request can have: 5 when its header is
   *   flexible (a correlation id and a tag buffer), else 4, which is also what an unknown id gets
   */
  minBodyOf(correlationId) {
    const request = this.requestQueue.inflight.get(correlationId)
    if (!request) {
      return Decoder.int32Size()
    }

    const { apiKey, apiVersion } = request.entry
    return responseHeaderVersion(apiKey, apiVersion) === RESPONSE_HEADER_V1
      ? Decoder.int32Size() + 1
      : Decoder.int32Size()
  }

  /**
   * @private
   */
  rejectRequests(error) {
    this.requestQueue.rejectAll(error)
  }
}
