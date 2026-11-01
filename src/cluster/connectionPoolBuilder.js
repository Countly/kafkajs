const { KafkaJSConnectionError, KafkaJSNonRetriableError } = require('../errors')
const ConnectionPool = require('../network/connectionPool')

/** `[host]:port` or `host:port`, the host holding no colon and no bracket in the second form. */
const BROKER_ADDRESS = /^(?:\[([^\]]+)\]|([^:[\]]+)):(\d+)$/

/**
 * Splits a bootstrap address into its host and port. An IPv6 literal must be bracketed
 * (`[::1]:9092`); the brackets are not part of the host handed to the socket factory.
 * @param {string} text the address as configured
 * @returns {{ host: string, port: number }}
 * @throws {KafkaJSNonRetriableError} for an unbracketed IPv6 literal, or a port that is missing,
 *   not an integer, or outside 1 to 65535
 */
const parseBroker = (text) => {
  const match = BROKER_ADDRESS.exec(text)
  const port = match ? Number(match[3]) : NaN

  if (!match || port < 1 || port > 65535) {
    const reason =
      !text.startsWith('[') && text.split(':').length > 2
        ? 'an IPv6 host must be written [::1]:9092'
        : 'expected host:port with a port from 1 to 65535'
    throw new KafkaJSNonRetriableError(`Failed to connect: invalid broker "${text}", ${reason}`)
  }

  return { host: match[1] ?? match[2], port }
}

/**
 * @typedef {Object} ConnectionPoolBuilder
 * @property {(destination?: { host?: string, port?: number, rack?: string }) => Promise<ConnectionPool>} build
 */

/**
 * @param {Object} options
 * @param {import("../../types").ISocketFactory} [options.socketFactory]
 * @param {string[]|(() => string[])} options.brokers
 * @param {Object} [options.ssl]
 * @param {Object} [options.sasl]
 * @param {string} options.clientId
 * @param {number} [options.drainTimeout] obsolete-resource drain bound
 * @param {number} options.requestTimeout
 * @param {boolean} [options.enforceRequestTimeout]
 * @param {number} [options.connectionTimeout]
 * @param {number} [options.connectionsMaxIdle] Idle milliseconds carried unchanged to each socket
 * @param {number|null} [options.maxInFlightRequests]
 * @param {number} [options.maxResponseSize] the response-size limit handed to every connection
 * @param {import("../../types").RetryOptions} [options.retry]
 * @param {import("../../types").Logger} options.logger
 * @param {import("../instrumentation/emitter")} [options.instrumentationEmitter]
 * @param {number} [options.reauthenticationThreshold]
 * @returns {ConnectionPoolBuilder}
 */
module.exports = ({
  socketFactory,
  brokers,
  ssl,
  sasl,
  clientId,
  requestTimeout,
  drainTimeout,
  enforceRequestTimeout,
  connectionTimeout,
  connectionTimeoutMax,
  reconnectBackoff,
  reconnectBackoffMax,
  setupPolicy,
  authenticationTimeout,
  connectionsMaxIdle,
  maxInFlightRequests,
  maxResponseSize,
  logger,
  instrumentationEmitter = null,
  reauthenticationThreshold,
}) => {
  let index = 0
  let bootstrapEndpoints = new Set()

  const isValidBroker = (broker) => {
    return broker && typeof broker === 'string' && broker.length > 0
  }

  const validateBrokers = (brokers) => {
    if (!brokers) {
      throw new KafkaJSNonRetriableError(`Failed to connect: brokers should not be null`)
    }

    if (Array.isArray(brokers)) {
      if (!brokers.length) {
        throw new KafkaJSNonRetriableError(`Failed to connect: brokers array is empty`)
      }

      brokers.forEach((broker, index) => {
        if (!isValidBroker(broker)) {
          throw new KafkaJSNonRetriableError(
            `Failed to connect: broker at index ${index} is invalid "${typeof broker}"`
          )
        }
        parseBroker(broker)
      })
    }
  }

  const getBrokers = async () => {
    let list

    if (typeof brokers === 'function') {
      try {
        list = await brokers()
      } catch (e) {
        const wrappedError = new KafkaJSConnectionError(
          `Failed to connect: "config.brokers" threw: ${e.message}`
        )
        wrappedError.stack = `${wrappedError.name}\n  Caused by: ${e.stack}`
        throw wrappedError
      }
    } else {
      list = brokers
    }

    validateBrokers(list)

    bootstrapEndpoints = new Set(
      list.map((text) => {
        const { host, port } = parseBroker(text)
        return `${host}:${port}`
      })
    )
    return list
  }

  return {
    /** @returns {Set<string>} copy of the complete latest validated bootstrap endpoints */
    get bootstrapEndpoints() {
      return new Set(bootstrapEndpoints)
    },
    build: async ({ host, port, rack } = {}) => {
      if (!host) {
        const list = await getBrokers()

        const seed = parseBroker(list[index++ % list.length])
        host = seed.host
        port = seed.port
      }

      return new ConnectionPool({
        host,
        port,
        rack,
        sasl,
        ssl,
        clientId,
        socketFactory,
        connectionTimeout,
        connectionTimeoutMax,
        reconnectBackoff,
        reconnectBackoffMax,
        setupPolicy,
        authenticationTimeout,
        connectionsMaxIdle,
        requestTimeout,
        drainTimeout,
        enforceRequestTimeout,
        maxInFlightRequests,
        maxResponseSize,
        instrumentationEmitter,
        logger,
        reauthenticationThreshold,
      })
    },
  }
}
