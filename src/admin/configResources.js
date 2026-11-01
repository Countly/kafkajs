const ConfigResourceTypes = require('../protocol/configResourceTypes')
const { KafkaJSInvalidConfigurationError, KafkaJSInvariantViolation } = require('../errors')
const { retrierOf, checked, waitOf } = require('./deadline')

/** Lists configuration resources without changing existing configuration method shapes. */
class ConfigResources {
  /** @param {object} context cluster and unchanged retry options @returns {ConfigResources} facade */
  constructor({ cluster, retry }) {
    this.cluster = cluster
    this.retry = retry
  }

  /** Lists resource identities, using only the explicit client-metrics v0 equivalence. @param {object} [options] resourceTypes @returns {Promise<object>} resources array @throws {Error} invalid filter, capability, malformed identity, deadline or broker failure */
  async listConfigResources(options = {}) {
    if (!options || typeof options !== 'object' || Array.isArray(options))
      throw new KafkaJSInvalidConfigurationError('Expected an options object')
    const types = options.resourceTypes ?? []
    if (
      options.resourceTypes === null ||
      !Array.isArray(types) ||
      new Set(types).size !== types.length ||
      types.some((t) => !Number.isInteger(t) || !Object.values(ConfigResourceTypes).includes(t))
    )
      throw new KafkaJSInvalidConfigurationError('Invalid resourceTypes')
    return retrierOf(this.retry)(async (bail) => {
      try {
        const broker = checked(await waitOf(() => this.cluster.findConnectedBroker()))
        const r = checked(await waitOf(() => broker.listConfigResources({ resourceTypes: types })))
        const seen = new Set()
        const resources = r.configResources.map(
          ({ resourceType = ConfigResourceTypes.CLIENT_METRICS, resourceName }) => {
            const key = JSON.stringify([resourceType, resourceName])
            if (
              !Number.isInteger(resourceType) ||
              typeof resourceName !== 'string' ||
              seen.has(key) ||
              (types.length && !types.includes(resourceType))
            )
              throw new KafkaJSInvariantViolation('Invalid configuration resource identity')
            seen.add(key)
            return { type: resourceType, name: resourceName }
          }
        )
        return { resources }
      } catch (error) {
        if (!error.retriable) bail(error)
        throw error
      }
    })
  }
}

/** Creates the bound method before the invocation wrapper. @param {object} context cluster/retry @returns {object} Admin methods */
module.exports = (context) => {
  const owner = new ConfigResources(context)
  return { listConfigResources: owner.listConfigResources.bind(owner) }
}
