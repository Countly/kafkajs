const { KafkaJSRequestTimeoutError, KafkaJSNonRetriableError } = require('../../errors')
const events = require('../instrumentationEvents')
const { isNativeError } = require('util').types
const describeThrown = require('../describeThrown')

const PRIVATE = {
  STATE: Symbol('private:SocketRequest:state'),
  EMIT_EVENT: Symbol('private:SocketRequest:emitEvent'),
}

const REQUEST_STATE = {
  PENDING: Symbol('PENDING'),
  SENT: Symbol('SENT'),
  COMPLETED: Symbol('COMPLETED'),
  REJECTED: Symbol('REJECTED'),
}

/**
 * SocketRequest abstracts the life cycle of a socket request, making it easier to track
 * request durations and to have individual timeouts per request.
 *
 * @typedef {Object} SocketRequest
 * @property {number} createdAt
 * @property {number} sentAt
 * @property {number} pendingDuration
 * @property {number} duration
 * @property {number} requestTimeout
 * @property {string} broker
 * @property {string} clientId
 * @property {RequestEntry} entry
 * @property {boolean} expectResponse
 * @property {Function} send
 * @property {Function} timeout
 * @property {Function} afterTimeout
 * @property {(frameLength?: number) => Error|null} guard
 *
 * @typedef {Object} RequestEntry
 * @property {string} apiKey
 * @property {string} apiName
 * @property {number} apiVersion
 * @property {number} correlationId
 * @property {Function} resolve
 * @property {Function} reject
 */
module.exports = class SocketRequest {
  /**
   * @param {Object} options
   * @param {number} options.requestTimeout
   * @param {string} options.broker - e.g: 127.0.0.1:9092
   * @param {string} options.clientId
   * @param {RequestEntry} options.entry
   * @param {boolean} options.expectResponse
   * @param {number} [options.frameLength] complete frame bytes, supplied to the transmission guard
   * @param {object|null} [options.telemetrySink=null] private numeric collector
   * @param {object|null} [options.telemetrySlot=null] captured slot reference
   * @param {Function} options.send
   * @param {() => void} options.timeout bookkeeping the queue does when this request times out
   * @param {(socketRequest: SocketRequest) => void} [options.afterTimeout] called once the request was rejected with its own
   *   timeout error, before the `NETWORK_REQUEST_TIMEOUT` event
   * @param {(frameLength?: number) => Error|null} [options.guard] checked by `refusal()` before the request is written
   * @param {import("../../instrumentation/emitter")} [options.instrumentationEmitter=null]
   * @param {import("../../../types").Logger} [options.logger] where a failing event listener is logged
   */
  constructor({
    requestTimeout,
    broker,
    clientId,
    entry,
    expectResponse,
    send,
    timeout,
    afterTimeout = null,
    guard = null,
    frameLength,
    telemetrySink = null,
    telemetrySlot = null,
    instrumentationEmitter = null,
    logger = null,
  }) {
    this.createdAt = Date.now()
    this.requestTimeout = requestTimeout
    this.broker = broker
    this.clientId = clientId
    this.entry = entry
    this.correlationId = entry.correlationId
    this.expectResponse = expectResponse
    this.sendRequest = send
    this.timeoutHandler = timeout
    this.afterTimeout = afterTimeout
    this.guard = guard
    this.frameLength = frameLength
    this.logger = logger
    this.telemetrySink = telemetrySink
    this.telemetrySlot = telemetrySlot

    this.sentAt = null
    this.duration = null
    this.pendingDuration = null

    this[PRIVATE.STATE] = REQUEST_STATE.PENDING
    this[PRIVATE.EMIT_EVENT] = (eventName, payload) =>
      instrumentationEmitter && instrumentationEmitter.emit(eventName, payload)
  }

  /**
   * @returns {Error|null} why this request must not be written, from its guard; null when it may be.
   *   A guard that throws refuses with the Error it threw, or, for another value, with a
   *   `KafkaJSNonRetriableError` it caused.
   */
  refusal() {
    if (!this.guard) {
      return null
    }

    try {
      return this.guard(this.frameLength)
    } catch (error) {
      return isNativeError(error)
        ? error
        : new KafkaJSNonRetriableError('The pre-write guard threw a value that is not an error', {
            cause: error,
          })
    }
  }

  send() {
    this.throwIfInvalidState({
      accepted: [REQUEST_STATE.PENDING],
      next: REQUEST_STATE.SENT,
    })

    this.sendRequest()
    this.sentAt = Date.now()
    this.pendingDuration = this.sentAt - this.createdAt
    this[PRIVATE.STATE] = REQUEST_STATE.SENT
  }

  /**
   * Rejects the request with its own `KafkaJSRequestTimeoutError` first, with no observer run, then
   * lets the owner act (`afterTimeout`), and only then emits `NETWORK_REQUEST_TIMEOUT`, contained.
   */
  timeoutRequest() {
    const { apiName, apiKey, apiVersion } = this.entry
    const requestInfo = `${apiName}(key: ${apiKey}, version: ${apiVersion})`
    const eventData = {
      broker: this.broker,
      clientId: this.clientId,
      correlationId: this.correlationId,
      createdAt: this.createdAt,
      sentAt: this.sentAt,
      pendingDuration: this.pendingDuration,
    }

    this.timeoutHandler()
    this.rejected(new KafkaJSRequestTimeoutError(`Request ${requestInfo} timed out`, eventData))
    this.afterTimeout?.(this)

    try {
      this[PRIVATE.EMIT_EVENT](events.NETWORK_REQUEST_TIMEOUT, {
        ...eventData,
        apiName,
        apiKey,
        apiVersion,
      })
    } catch (error) {
      try {
        this.logger?.error('A request timeout listener failed', {
          broker: this.broker,
          correlationId: this.correlationId,
          error: describeThrown(error),
          cause: error,
        })
      } catch {}
    }
  }

  completed({ size, payload }) {
    this.throwIfInvalidState({
      accepted: [REQUEST_STATE.SENT],
      next: REQUEST_STATE.COMPLETED,
    })

    const { entry, correlationId, broker, clientId, createdAt, sentAt, pendingDuration } = this

    this[PRIVATE.STATE] = REQUEST_STATE.COMPLETED
    this.duration = Date.now() - this.sentAt
    entry.resolve({ correlationId, entry, size, payload })

    if (this.expectResponse && this.telemetrySink) {
      try {
        this.telemetrySink.observeRequest(this.telemetrySlot, this.duration)
      } catch {}
    }

    this[PRIVATE.EMIT_EVENT](events.NETWORK_REQUEST, {
      broker,
      clientId,
      correlationId,
      size,
      createdAt,
      sentAt,
      pendingDuration,
      duration: this.duration,
      apiName: entry.apiName,
      apiKey: entry.apiKey,
      apiVersion: entry.apiVersion,
    })
  }

  rejected(error) {
    this.throwIfInvalidState({
      accepted: [REQUEST_STATE.PENDING, REQUEST_STATE.SENT],
      next: REQUEST_STATE.REJECTED,
    })

    this[PRIVATE.STATE] = REQUEST_STATE.REJECTED
    this.duration = Date.now() - this.sentAt
    this.entry.reject(error)
  }

  /**
   * @private
   */
  throwIfInvalidState({ accepted, next }) {
    if (accepted.includes(this[PRIVATE.STATE])) {
      return
    }

    const current = this[PRIVATE.STATE].toString()

    throw new KafkaJSNonRetriableError(
      `Invalid state, can't transition from ${current} to ${next.toString()}`
    )
  }
}
