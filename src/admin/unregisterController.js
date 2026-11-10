const { KafkaJSInvalidConfigurationError } = require('../errors')
const apiKeys = require('../protocol/requests/apiKeys')
const { requests } = require('../protocol/requests')
const { retrierOf, checked, waitOf } = require('./deadline')

const MAX_INT32 = 2147483647

/** Removes a controller registration (KIP-1312) on the first broker that offers the API. */
class ControllerRegistration {
  /** @param {object} context cluster and unchanged retry options @returns {ControllerRegistration} facade */
  constructor({ cluster, retry }) {
    this.cluster = cluster
    this.retry = retry
  }

  /**
   * Unregisters one controller id on the first known broker, lowest node id first, that negotiated
   * RPC 94. Retriable broker and transport errors retry under the inherited deadline; a broker
   * error code is rethrown as its typed protocol error.
   * @param {{controllerId: number}} options the non-negative int32 id to remove
   * @returns {Promise<void>} resolves once the broker reports success
   * @throws {KafkaJSInvalidConfigurationError} for a malformed id, before any lookup or send
   * @throws {KafkaJSServerDoesNotSupportApiKey} when no known broker offers RPC 94
   * @throws {KafkaJSProtocolError} for a broker error, such as CONTROLLER_ID_NOT_REGISTERED
   * @throws {Error} deadline, retry exhaustion or transport failure
   */
  async unregisterController(options) {
    const controllerId = this.controllerId(options)
    return retrierOf(this.retry)(async (bail) => {
      try {
        const broker = checked(
          await waitOf(() =>
            this.cluster.findBrokerSupporting(
              apiKeys.UnregisterController,
              requests.UnregisterController
            )
          )
        )
        checked(await waitOf(() => broker.unregisterController({ controllerId })))
      } catch (error) {
        if (!error.retriable) bail(error)
        throw error
      }
    })
  }

  /**
   * @param {object} options caller input
   * @returns {number} the validated controller id
   * @throws {KafkaJSInvalidConfigurationError} unless options is an object holding an int32 id >= 0
   */
  controllerId(options) {
    const id = options?.controllerId
    if (!Number.isInteger(id) || id < 0 || id > MAX_INT32)
      throw new KafkaJSInvalidConfigurationError(
        'controllerId must be an integer from 0 to 2147483647'
      )
    return id
  }
}

/**
 * Creates the bound method before the invocation wrapper.
 * @param {object} context cluster and retry options
 * @returns {{unregisterController: Function}} Admin methods
 */
module.exports = (context) => {
  const owner = new ControllerRegistration(context)
  return { unregisterController: owner.unregisterController.bind(owner) }
}
