const createSocket = require('./socket')
const createRequest = require('../protocol/request')
const Decoder = require('../protocol/decoder')
const { NETWORK_AUTHENTICATION_FAILURE } = require('./instrumentationEvents')
const { KafkaJSConnectionError, KafkaJSConnectionClosedError } = require('../errors')
const { INT_32_MAX_VALUE } = require('../constants')
const getEnv = require('../env')
const RequestQueue = require('./requestQueue')
const { CONNECTION_STATUS, CONNECTED_STATUS } = require('./connectionStatus')
const sharedPromiseTo = require('../utils/sharedPromiseTo')
const Long = require('../utils/long')
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
   * @param {number} options.connectionTimeout The connection timeout, in milliseconds
   * @param {boolean} [options.enforceRequestTimeout]
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
    enforceRequestTimeout = true,
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
    this.reauthenticationThreshold = reauthenticationThreshold
    this.instrumentationEmitter = instrumentationEmitter
    this.onAuthenticationFailure = onAuthenticationFailure

    this.bytesBuffered = 0
    this.bytesNeeded = Decoder.int32Size()
    this.chunks = []

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

    /** Settles once the latest request has reached the request queue, whatever its outcome. */
    this[PRIVATE.QUEUED] = Promise.resolve()

    /**
     * @private
     * @returns {Promise}
     */
    this[PRIVATE.AUTHENTICATE] = sharedPromiseTo(async () => {
      if (this.sasl && !this.isAuthenticated()) {
        const authenticator = new SASLAuthenticator(this, this.logger, this.versions)

        await authenticator.authenticate()
        this.authenticatedAt = process.hrtime()
        this.sessionLifetime = Long.fromValue(authenticator.sessionLifetime)
      }
    })

    /**
     * @private
     * @returns {Promise}
     */
    this[PRIVATE.CONNECT] = sharedPromiseTo(() => this[PRIVATE.CONNECT_ATTEMPT]())
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
   * @returns {Promise}
   */
  connect() {
    return this[PRIVATE.CONNECT]()
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

      let timeoutId
      const attempt = ++this.connectAttempt
      const isCurrentAttempt = () => this.connectAttempt === attempt

      const abort = (error) => {
        if (!isCurrentAttempt()) return
        clearTimeout(timeoutId)
        this[PRIVATE.ABORT_CONNECT] = null
        reject(error)
      }
      this[PRIVATE.ABORT_CONNECT] = abort

      const onConnect = () => {
        if (!isCurrentAttempt()) return
        clearTimeout(timeoutId)
        this[PRIVATE.ABORT_CONNECT] = null
        this.connectionStatus = CONNECTION_STATUS.CONNECTED
        this.requestQueue.scheduleRequestTimeoutCheck()
        this.generation += 1
        resolve(true)
      }

      const onData = (data) => {
        if (!isCurrentAttempt()) return
        this.processData(data)
      }

      const onEnd = async () => {
        if (!isCurrentAttempt()) return
        clearTimeout(timeoutId)

        const wasConnected = this.isConnected()

        if (wasConnected) {
          this.logDebug('Kafka server has closed connection')
          this.rejectRequests(
            new KafkaJSConnectionClosedError('Closed connection', {
              host: this.host,
              port: this.port,
            })
          )
        }

        // Deliberately does not clear ABORT_CONNECT: if this fires before the socket ever
        // connected, the disconnect() below finds it still set and rejects this attempt's
        // promise instead of leaving it to hang past its own timeout (already cleared above).
        await this.disconnect()
      }

      const onError = async (e) => {
        if (!isCurrentAttempt()) return
        clearTimeout(timeoutId)
        this[PRIVATE.ABORT_CONNECT] = null

        const error = new KafkaJSConnectionError(`Connection error: ${e.message}`, {
          broker: `${this.host}:${this.port}`,
          code: e.code,
        })

        this.logError(error.message, { stack: e.stack })
        this.rejectRequests(error)
        await this.disconnect()

        reject(error)
      }

      const onTimeout = async () => {
        if (!isCurrentAttempt()) return
        this[PRIVATE.ABORT_CONNECT] = null

        const error = new KafkaJSConnectionError('Connection timeout', {
          broker: `${this.host}:${this.port}`,
        })

        this.logError(error.message)
        this.rejectRequests(error)
        await this.disconnect()
        reject(error)
      }

      this.logDebug(`Connecting`, {
        ssl: !!this.ssl,
        sasl: !!this.sasl,
      })

      try {
        timeoutId = setTimeout(onTimeout, this.connectionTimeout)
        this.socket = createSocket({
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
      } catch (e) {
        clearTimeout(timeoutId)
        this[PRIVATE.ABORT_CONNECT] = null
        reject(
          new KafkaJSConnectionError(`Failed to connect: ${e.message}`, {
            broker: `${this.host}:${this.port}`,
          })
        )
      }
    })
  }

  /**
   * Tears down the current socket, rejecting any in-flight `connect()` attempt it supersedes
   * (KIP-35 rule 7 depends on a reconnect never leaving a caller hanging past its timeout) with a
   * plain, retriable `KafkaJSConnectionError` — not `KafkaJSConnectionClosedError`, which several
   * `src/cluster/` branches don't recognise by name, since it would otherwise skip seed-broker
   * rotation and metadata refresh on a connect that was merely superseded, not actually refused.
   * `data`/`end`/`timeout` are detached from the socket before it is closed: those events are
   * only ever meaningful to the attempt that owned the socket, and the peer's own FIN can be
   * delivered asynchronously, possibly after a new socket has already replaced this one. `error`
   * stays attached — inert once this attempt is no longer current — so a post-close error on the
   * old socket (e.g. `ECONNRESET`) cannot become an uncaught exception. The socket to tear down
   * and the epoch to downgrade to DISCONNECTED are both captured up front, before
   * `waitForPendingRequests()` yields: a concurrent `disconnect()`/`connect()` pair can otherwise
   * install a new socket (and move the status past DISCONNECTING) while this call is still
   * draining, and this call's continuation must act on the socket and epoch it actually owns, not
   * on whatever `this.socket`/`this.connectAttempt` happen to hold once it resumes. A socket the
   * broker already destroyed is left alone, so repeated calls add no listeners to it.
   *
   * @public
   * @returns {Promise}
   */
  async disconnect() {
    this.authenticatedAt = null
    this.connectionStatus = CONNECTION_STATUS.DISCONNECTING
    this.logDebug('disconnecting...')

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
    await this.requestQueue.waitForPendingRequests()

    // A newer connect()/disconnect() pair that ran while the drain was yielding already owns
    // this connection's socket and status; this disconnect() has nothing left to do.
    if (this.connectAttempt !== epoch) {
      return true
    }

    // Ends the attempt's epoch: every handler still attached to the socket torn down below
    // (possibly mid-connect, possibly long since connected) recognises itself as stale from here.
    this.connectAttempt += 1
    this.requestQueue.destroy()

    // A socket already closed is left alone: unref() on it queues a 'connect' listener that never
    // fires, one per disconnect() call.
    if (socket && !socket.destroyed) {
      // socketFactory's documented contract is a real net.Socket, which always has this; guarded
      // anyway since nothing else here needs more than end()/unref() from the socket.
      if (typeof socket.removeAllListeners === 'function') {
        socket.removeAllListeners('data')
        socket.removeAllListeners('end')
        socket.removeAllListeners('timeout')
      }
      socket.end()
      socket.unref()
    }

    this.connectionStatus = CONNECTION_STATUS.DISCONNECTED
    this.logDebug('disconnected')
    return true
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

    if (this.authenticatedAt == null) {
      return true
    }

    const [secondsSince, remainingNanosSince] = process.hrtime(this.authenticatedAt)
    const millisSince = Long.fromValue(secondsSince)
      .multiply(1000)
      .add(Long.fromValue(remainingNanosSince).divide(1000000))

    const reauthenticateAt = millisSince.add(this.reauthenticationThreshold)
    return reauthenticateAt.greaterThanOrEqual(this.sessionLifetime)
  }

  /** @public */
  async authenticate() {
    await this[PRIVATE.AUTHENTICATE]()
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
   * @returns {Promise<data>} where data is the return of "response#parse"
   */
  async send({ request, response, requestTimeout = null, logResponseError = true }) {
    const expectResponse = !request.expectResponse || request.expectResponse()
    const prepared = this[PRIVATE.PREPARE]({ request, expectResponse })
    const enqueue = (encoded) => this[PRIVATE.ENQUEUE](encoded, requestTimeout)

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

      /**
       * @see KIP-219
       * If the response indicates that the client-side needs to throttle, do that.
       */
      this.requestQueue.maybeThrottle(payloadDecoded.clientSideThrottleTime)

      const data = await response.parse(payloadDecoded)
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
   * @param {{ request: object, expectResponse: boolean }} options
   * @returns {Promise<{ request: object, expectResponse: boolean, correlationId: number,
   *   requestPayload: object }>}
   */
  async [PRIVATE.PREPARE]({ request, expectResponse }) {
    if (!this.isAuthenticated() && isAuthenticatedRequest(request)) {
      await this[PRIVATE.AUTHENTICATE]()
    }

    this.failIfNotConnected()

    const { clientId } = this
    const correlationId = this.nextCorrelationId()
    const requestPayload = await createRequest({ request, correlationId, clientId })
    this.logDebug(`Request ${requestInfo(request)}`, {
      correlationId,
      expectResponse,
      size: Buffer.byteLength(requestPayload.buffer),
    })

    return { request, expectResponse, correlationId, requestPayload }
  }

  /**
   * Pushes an encoded request into the request queue.
   * @private
   * @param {{ request: object, expectResponse: boolean, correlationId: number,
   *   requestPayload: object }} encoded what `PREPARE` resolved with
   * @param {number|null} requestTimeout
   * @returns {{ answered: Promise<{ correlationId: number, size: number, entry: object,
   *   payload: Buffer }> }} `answered` settles with the response frame; wrapped so that waiting
   *   for the push does not wait for the response
   */
  [PRIVATE.ENQUEUE]({ request, expectResponse, correlationId, requestPayload }, requestTimeout) {
    const { apiKey, apiName, apiVersion } = request
    const answered = new Promise((resolve, reject) => {
      try {
        this.failIfNotConnected()
        const entry = { apiKey, apiName, apiVersion, correlationId, resolve, reject }

        this.requestQueue.push({
          entry,
          expectResponse,
          requestTimeout,
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
   */
  failIfNotConnected() {
    if (this.connectionStatus !== CONNECTION_STATUS.CONNECTED) {
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
   * @private
   */
  processData(rawData) {
    // Accumulate the new chunk
    this.chunks.push(rawData)
    this.bytesBuffered += Buffer.byteLength(rawData)

    // Process data if there are enough bytes to read the expected response size,
    // otherwise keep buffering
    while (this.bytesNeeded <= this.bytesBuffered) {
      const buffer = this.chunks.length > 1 ? Buffer.concat(this.chunks) : this.chunks[0]
      const decoder = new Decoder(buffer)
      const expectedResponseSize = decoder.readInt32()

      // Return early if not enough bytes to read the full response
      if (!decoder.canReadBytes(expectedResponseSize)) {
        this.chunks = [buffer]
        this.bytesBuffered = Buffer.byteLength(buffer)
        this.bytesNeeded = Decoder.int32Size() + expectedResponseSize
        return
      }

      const response = new Decoder(decoder.readBytes(expectedResponseSize))

      // Reset the buffered chunks as the rest of the bytes
      const remainderBuffer = decoder.readAll()
      this.chunks = [remainderBuffer]
      this.bytesBuffered = Buffer.byteLength(remainderBuffer)
      this.bytesNeeded = Decoder.int32Size()

      const correlationId = response.readInt32()
      const payload = response.readAll()

      this.requestQueue.fulfillRequest({
        size: expectedResponseSize,
        correlationId,
        payload,
      })
    }
  }

  /**
   * @private
   */
  rejectRequests(error) {
    this.requestQueue.rejectAll(error)
  }
}
