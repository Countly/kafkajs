const { currentDeadline } = require('../../admin/deadline')
const { EventEmitter } = require('events')
const SocketRequest = require('./socketRequest')
const describeThrown = require('../describeThrown')
const events = require('../instrumentationEvents')
const { KafkaJSInvariantViolation } = require('../../errors')

const PRIVATE = {
  EMIT_QUEUE_SIZE_EVENT: Symbol('private:RequestQueue:emitQueueSizeEvent'),
  EMIT_REQUEST_QUEUE_EMPTY: Symbol('private:RequestQueue:emitQueueEmpty'),
}

const REQUEST_QUEUE_EMPTY = 'requestQueueEmpty'
const CHECK_PENDING_REQUESTS_INTERVAL = 10

module.exports = class RequestQueue extends EventEmitter {
  /**
   * @param {Object} options
   * @param {number|null} options.maxInFlightRequests
   * @param {number} options.requestTimeout
   * @param {boolean} options.enforceRequestTimeout
   * @param {string} options.clientId
   * @param {string} options.broker
   * @param {import("../../../types").Logger} options.logger
   * @param {import("../../instrumentation/emitter")} [options.instrumentationEmitter=null]
   * @param {() => boolean} [options.isConnected]
   * @param {(socketRequest: SocketRequest) => void} [options.onRequestTimeout] Called when a request
   *   exceeds its timeout, after that request was rejected with its own timeout error and before
   *   the `NETWORK_REQUEST_TIMEOUT` event. The queue does nothing else about the timeout: the owner
   *   decides what happens to the requests that remain (the connection fails its generation).
   */
  constructor({
    instrumentationEmitter = null,
    maxInFlightRequests,
    requestTimeout,
    enforceRequestTimeout,
    clientId,
    broker,
    logger,
    isConnected = () => true,
    onRequestTimeout = () => {},
  }) {
    super()
    this.instrumentationEmitter = instrumentationEmitter
    this.maxInFlightRequests = maxInFlightRequests
    this.requestTimeout = requestTimeout
    this.enforceRequestTimeout = enforceRequestTimeout
    this.clientId = clientId
    this.broker = broker
    this.logger = logger
    this.isConnected = isConnected
    this.onRequestTimeout = onRequestTimeout
    this.afterTimeout = (socketRequest) => {
      this.onRequestTimeout(socketRequest)
      this.emitQueueState()
    }

    this.inflight = new Map()
    this.pending = []
    this.lastActivityAt = Date.now()

    /**
     * Until when this request queue is throttled and shouldn't send requests
     *
     * The value represents the timestamp of the end of the throttling in ms-since-epoch. If the value
     * is smaller than the current timestamp no throttling is active.
     *
     * @type {number}
     */
    this.throttledUntil = -1

    /**
     * Timeout id if we have scheduled a check for pending requests due to client-side throttling
     *
     * @type {null|NodeJS.Timeout}
     */
    this.throttleCheckTimeoutId = null

    this[PRIVATE.EMIT_REQUEST_QUEUE_EMPTY] = () => {
      if (this.pending.length === 0 && this.inflight.size === 0) {
        if (this.listenerCount(REQUEST_QUEUE_EMPTY) === 0) {
          return
        }
        for (const listener of this.rawListeners(REQUEST_QUEUE_EMPTY)) {
          try {
            listener.call(this)
          } catch (error) {
            this.reportObserverFailure(error)
          }
        }
      }
    }

    this[PRIVATE.EMIT_QUEUE_SIZE_EVENT] = () => {
      instrumentationEmitter &&
        instrumentationEmitter.emit(events.NETWORK_REQUEST_QUEUE_SIZE, {
          broker: this.broker,
          clientId: this.clientId,
          queueSize: this.pending.length,
        })

      this[PRIVATE.EMIT_REQUEST_QUEUE_EMPTY]()
    }
  }

  /**
   * @public
   */
  scheduleRequestTimeoutCheck() {
    if (this.enforceRequestTimeout) {
      this.destroy()

      this.requestTimeoutIntervalId = setInterval(
        () => {
          this.inflight.forEach((request) => {
            if (Date.now() - request.sentAt > request.requestTimeout) {
              request.timeoutRequest()
            }
          })

          if (!this.isConnected()) {
            this.destroy()
          }
        },
        Math.min(this.requestTimeout, 100)
      )
    }
  }

  maybeThrottle(clientSideThrottleTime) {
    if (clientSideThrottleTime !== null && clientSideThrottleTime > 0) {
      this.logger.debug(`Client side throttling in effect for ${clientSideThrottleTime}ms`)
      const minimumThrottledUntil = Date.now() + clientSideThrottleTime
      this.throttledUntil = Math.max(minimumThrottledUntil, this.throttledUntil)
    }
  }

  createSocketRequest(pushedRequest) {
    const { correlationId } = pushedRequest.entry
    const defaultRequestTimeout = this.requestTimeout
    const customRequestTimeout = pushedRequest.requestTimeout

    // Some protocol requests have custom request timeouts (e.g JoinGroup, Fetch, etc). The custom
    // timeouts are influenced by user configurations, which can be lower than the default requestTimeout
    const requestTimeout = Math.max(defaultRequestTimeout, customRequestTimeout || 0)

    const socketRequest = new SocketRequest({
      entry: pushedRequest.entry,
      expectResponse: pushedRequest.expectResponse,
      broker: this.broker,
      clientId: this.clientId,
      instrumentationEmitter: this.instrumentationEmitter,
      logger: this.logger,
      guard: pushedRequest.guard,
      frameLength: pushedRequest.frameLength,
      requestTimeout,
      send: () => {
        if (this.inflight.has(correlationId)) {
          throw new KafkaJSInvariantViolation('Correlation id already exists')
        }
        this.inflight.set(correlationId, socketRequest)
        pushedRequest.sendRequest()
      },
      timeout: () => {
        this.inflight.delete(correlationId)
      },
      afterTimeout: this.afterTimeout,
    })

    return socketRequest
  }

  /**
   * @typedef {Object} PushedRequest
   * @property {import("./socketRequest").RequestEntry} entry
   * @property {boolean} expectResponse
   * @property {number} [frameLength] complete encoded frame bytes, including the length prefix
   * @property {Function} sendRequest
   * @property {number} [requestTimeout]
   * @property {(frameLength?: number) => Error|null} [guard] checked just before the request is written; an error it
   *   returns rejects the request, which is then written nowhere and takes no in-flight slot
   *
   * @public
   * @param {PushedRequest} pushedRequest
   */
  push(pushedRequest) {
    this.lastActivityAt = Date.now()
    const { correlationId } = pushedRequest.entry
    const operation = currentDeadline()
    operation?.check()
    let unown = () => {}
    if (operation) {
      const entry = pushedRequest.entry
      pushedRequest = {
        ...pushedRequest,
        entry: {
          ...entry,
          resolve: (value) => {
            unown()
            entry.resolve(value)
          },
          reject: (error) => {
            unown()
            entry.reject(error)
          },
        },
      }
    }
    const socketRequest = this.createSocketRequest(pushedRequest)
    let enrolling = true
    let refused = false
    if (operation)
      unown = operation.own((error) => {
        const index = this.pending.indexOf(socketRequest)
        if (index < 0 && !enrolling) return
        refused = true
        if (index >= 0) this.pending.splice(index, 1)
        socketRequest.rejected(error)
        if (this.pending.length === 0) {
          clearTimeout(this.throttleCheckTimeoutId)
          this.throttleCheckTimeoutId = null
        }
        this.emitQueueState()
      })
    if (refused) {
      unown()
      return
    }
    operation?.check()

    if (this.canSendSocketRequestImmediately()) {
      operation?.check()
      enrolling = false
      this.sendSocketRequest(socketRequest)
      return
    }

    operation?.check()
    this.pending.push(socketRequest)
    enrolling = false
    this.scheduleCheckPendingRequests()

    this.logger.debug(`Request enqueued`, {
      clientId: this.clientId,
      broker: this.broker,
      correlationId,
    })

    this[PRIVATE.EMIT_QUEUE_SIZE_EVENT]()
  }

  /**
   * @param {SocketRequest} socketRequest
   */
  sendSocketRequest(socketRequest) {
    const refusal = socketRequest.refusal()
    if (refusal) {
      socketRequest.rejected(refusal)
      return
    }

    socketRequest.send()

    if (!socketRequest.expectResponse) {
      this.lastActivityAt = socketRequest.sentAt
      this.logger.debug(`Request does not expect a response, resolving immediately`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId: socketRequest.correlationId,
      })

      this.inflight.delete(socketRequest.correlationId)
      socketRequest.completed({ size: 0, payload: null })
    }
  }

  /**
   * Completes the request the response belongs to, after sending the pending requests the freed
   * slot admits. The request has left the in-flight set by then and no teardown can reach it, so
   * it is completed even when sending a pending request, or an observer of that, throws: the
   * valid response is delivered and the exception then propagates to the caller.
   * @public
   * @param {object} response
   * @param {number} response.correlationId
   * @param {Buffer} response.payload
   * @param {number} response.size
   * @throws {Error} what sending a pending request, or an observer of that, threw
   */
  fulfillRequest({ correlationId, payload, size }) {
    this.lastActivityAt = Date.now()
    const socketRequest = this.inflight.get(correlationId)
    this.inflight.delete(correlationId)

    try {
      this.checkPendingRequests()
    } finally {
      if (socketRequest) {
        socketRequest.completed({ size, payload })
      } else {
        this.logger.warn(`Response without match`, {
          clientId: this.clientId,
          broker: this.broker,
          correlationId,
        })
      }
    }

    this[PRIVATE.EMIT_REQUEST_QUEUE_EMPTY]()
  }

  /**
   * @public
   * @param {Error} error
   */
  rejectAll(error) {
    this.settleAll(error)
    this.emitQueueState()
  }

  /**
   * Rejects every in-flight and pending request, in send order, with `error`, after the queue was
   * emptied. A rejection runs no observer, so the queue is settled before anything else can run.
   * @public
   * @param {Error} error
   */
  settleAll(error) {
    const requests = [...this.inflight.values(), ...this.pending]
    this.pending = []
    this.inflight.clear()

    for (const socketRequest of requests) {
      socketRequest.rejected(error)
    }
  }

  /**
   * Tells the observers the queue changed: the queue-size event, then the queue-empty event that
   * releases `waitForPendingRequests`. Each runs in its own `try`, so a listener that throws
   * stops neither the other nor its caller; the failure is logged.
   * @public
   */
  emitQueueState() {
    for (const emit of [
      this[PRIVATE.EMIT_QUEUE_SIZE_EVENT],
      this[PRIVATE.EMIT_REQUEST_QUEUE_EMPTY],
    ]) {
      try {
        emit()
      } catch (error) {
        this.reportObserverFailure(error)
      }
    }
  }

  /**
   * Reports an observer failure without allowing a diagnostic to interrupt settlement.
   * @private
   * @param {*} error whatever the observer threw
   * @returns {void}
   */
  reportObserverFailure(error) {
    try {
      this.logger.error('A queue observer failed', {
        broker: this.broker,
        error: describeThrown(error),
        cause: error,
      })
    } catch {}
  }

  /**
   * Reports whether neither a queued nor an in-flight request exists. Encoding
   * belongs to Connection and is checked there before this result permits reaping.
   * @public
   * @returns {boolean} true only when both populations are empty
   */
  isIdle() {
    return this.pending.length === 0 && this.inflight.size === 0
  }

  /**
   * @public
   */
  waitForPendingRequests() {
    return new Promise((resolve) => {
      if (this.pending.length === 0 && this.inflight.size === 0) {
        return resolve()
      }

      this.logger.debug('Waiting for pending requests', {
        clientId: this.clientId,
        broker: this.broker,
        currentInflightRequests: this.inflight.size,
        currentPendingQueueSize: this.pending.length,
      })

      this.once(REQUEST_QUEUE_EMPTY, () => resolve())
    })
  }

  /**
   * @public
   */
  destroy() {
    clearInterval(this.requestTimeoutIntervalId)
    clearTimeout(this.throttleCheckTimeoutId)
    this.throttleCheckTimeoutId = null
  }

  canSendSocketRequestImmediately() {
    const shouldEnqueue =
      (this.maxInFlightRequests != null && this.inflight.size >= this.maxInFlightRequests) ||
      this.throttledUntil > Date.now()

    return !shouldEnqueue
  }

  /**
   * Check and process pending requests either now or in the future
   *
   * This function will send out as many pending requests as possible taking throttling and
   * in-flight limits into account.
   */
  checkPendingRequests() {
    while (this.pending.length > 0 && this.canSendSocketRequestImmediately()) {
      const pendingRequest = this.pending.shift() // first in first out
      this.sendSocketRequest(pendingRequest)

      this.logger.debug(`Consumed pending request`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId: pendingRequest.correlationId,
        pendingDuration: pendingRequest.pendingDuration,
        currentPendingQueueSize: this.pending.length,
      })

      this[PRIVATE.EMIT_QUEUE_SIZE_EVENT]()
    }

    this.scheduleCheckPendingRequests()
  }

  /**
   * Ensure that pending requests will be checked in the future
   *
   * Reconciles upstream PRs #1755, #1768, #1667 and #1572 with Countly's production patch:
   * an idle, unthrottled queue schedules nothing (#1704/#1556 busy loop) and the delay is
   * always non-negative (#1751 TimeoutNegativeWarning on Node >= 24).
   */
  scheduleCheckPendingRequests() {
    // If there is already something scheduled we assume that it will be fine, and potentially
    // fix up a new timeout if needed at that time.
    if (this.throttleCheckTimeoutId) {
      return
    }

    const timeUntilUnthrottled = this.throttledUntil - Date.now()
    const isThrottled = timeUntilUnthrottled > 0

    // Nothing is waiting and no throttle to wait out: there is nothing for a future check to do.
    // Note that if we're merely "overloaded" by having too many inflight requests we will
    // anyway check the queue when one of them gets fulfilled.
    if (this.pending.length === 0 && !isThrottled) {
      return
    }

    const scheduleAt = isThrottled
      ? Math.max(timeUntilUnthrottled, CHECK_PENDING_REQUESTS_INTERVAL)
      : CHECK_PENDING_REQUESTS_INTERVAL

    this.throttleCheckTimeoutId = setTimeout(() => {
      this.throttleCheckTimeoutId = null
      this.checkPendingRequests()
    }, scheduleAt)
  }
}
