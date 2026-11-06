const {
  KafkaJSInvalidConfigurationError,
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSInvariantViolation,
} = require('../errors')
const { retrierOf, checked, waitOf, currentDeadline } = require('./deadline')
const { itemRetry } = require('./itemRetry')

/** Validates a field without echoing caller data.
 * @param {boolean} condition accepted shape
 * @param {string} field fixed field name
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} invalid shape
 */
const valid = (condition, field) => {
  if (!condition) throw new KafkaJSInvalidConfigurationError(`Invalid ${field}`)
}
/** Canonicalizes an entity identity independently of tuple order.
 * @param {object} entry quota entry
 * @returns {string} collision-free tuple identity
 */
const keyOf = ({ entity }) =>
  JSON.stringify(
    entity
      .map(({ entityType, entityName }) => [entityType, entityName])
      .sort((a, b) => a[0].localeCompare(b[0]))
  )
/** Whether a failure must reject the whole additive operation.
 * @param {Error} error operation failure
 * @returns {boolean} capability, validation, invariant or original deadline refusal
 */
const callFailure = (error) =>
  error instanceof KafkaJSServerDoesNotSupportApiKey ||
  error instanceof KafkaJSInvalidConfigurationError ||
  error instanceof KafkaJSInvariantViolation ||
  error === currentDeadline()?.refusal()

/** Quota validation and routing, with one inherited deadline and per-entry retry. */
class ClientQuotas {
  /** Binds the existing cluster and retry policy; call state stays local to each method.
   * @param {object} options shared cluster and unchanged retry policy
   */
  constructor({ cluster, retry }) {
    this.cluster = cluster
    this.retry = retry
  }

  /** Reads matching quota entities without changing metadata or quota caches.
   * @param {object} options components, strict and inherited apiTimeout
   * @returns {Promise<object[]>} entities and numeric quota values
   * @throws {Error} invalid filters, capability, top-level broker failure or deadline
   */
  async describeClientQuotas(options = {}) {
    valid(options != null && typeof options === 'object' && !Array.isArray(options), 'options')
    const { components, strict = false } = options
    valid(Array.isArray(components), 'components')
    valid(typeof strict === 'boolean', 'strict')
    const seen = new Set()
    const filters = Array.from(components, (component) => {
      valid(component && ['user', 'client-id', 'ip'].includes(component.entityType), 'entityType')
      const { entityType, matchType, match } = component
      valid(!seen.has(entityType), 'duplicate entityType')
      seen.add(entityType)
      valid(['exact', 'default', 'any'].includes(matchType), 'matchType')
      valid(
        matchType === 'exact' ? typeof match === 'string' && match.length > 0 : match === undefined,
        'match'
      )
      return {
        entityType,
        matchType: { exact: 0, default: 1, any: 2 }[matchType],
        match: match ?? null,
      }
    })
    return retrierOf(this.retry)(async (bail) => {
      try {
        const broker = checked(await waitOf(() => this.cluster.findConnectedBroker()))
        const result = checked(
          await waitOf(() => broker.describeClientQuotas({ components: filters, strict }))
        )
        const identities = new Set()
        return (result.entries ?? []).map(({ entity, values }) => {
          const key = keyOf({ entity })
          if (identities.has(key)) throw new KafkaJSInvariantViolation('Duplicate quota entity')
          identities.add(key)
          if (new Set(values.map(({ key }) => key)).size !== values.length)
            throw new KafkaJSInvariantViolation('Duplicate quota key')
          return {
            entity,
            values: Object.fromEntries(values.map(({ key, value }) => [key, value])),
          }
        })
      } catch (error) {
        if (error.retriable === false) bail(error)
        throw error
      }
    })
  }

  /** Validates every quota operation before discovery; only unresolved entries are retried.
   * @param {object} options entries and validateOnly
   * @returns {Promise<object[]>} caller-ordered entity results with typed errors as data
   * @throws {Error} invalid input, unsupported capability, invariant or API deadline
   */
  async alterClientQuotas(options = {}) {
    valid(options != null && typeof options === 'object' && !Array.isArray(options), 'options')
    const { entries, validateOnly = false } = options
    valid(Array.isArray(entries) && entries.length > 0, 'entries')
    valid(typeof validateOnly === 'boolean', 'validateOnly')
    const seen = new Set()
    const wire = new Map()
    for (const entry of entries) {
      valid(entry && Array.isArray(entry.entity) && entry.entity.length > 0, 'entity')
      const types = new Set()
      const entity = Array.from(entry.entity, (part) => {
        valid(part && ['user', 'client-id', 'ip'].includes(part.entityType), 'entityType')
        valid(!types.has(part.entityType), 'duplicate entityType')
        types.add(part.entityType)
        valid(part.entityName === null || typeof part.entityName === 'string', 'entityName')
        return { entityType: part.entityType, entityName: part.entityName }
      })
      valid(Array.isArray(entry.ops) && entry.ops.length > 0, 'ops')
      const keys = new Set()
      const ops = Array.from(entry.ops, (op) => {
        valid(
          op && typeof op.key === 'string' && op.key.length > 0 && !keys.has(op.key),
          'quota key'
        )
        keys.add(op.key)
        valid(op.remove === undefined || typeof op.remove === 'boolean', 'remove')
        valid(
          op.remove === true
            ? !Object.hasOwn(op, 'value')
            : typeof op.value === 'number' && Number.isFinite(op.value) && op.value >= 0,
          'quota value'
        )
        return { key: op.key, value: op.remove === true ? 0 : op.value, remove: op.remove ?? false }
      })
      const key = keyOf({ entity })
      valid(!seen.has(key), 'duplicate entity')
      seen.add(key)
      wire.set(key, { entity, ops })
    }
    return itemRetry(
      {
        items: [...wire.values()].map(({ entity }) => ({ entity })),
        keyOf,
        retry: this.retry,
        retryable: (row) => row.errorCode === 7,
        callFailure,
        sanitize: (message) => message,
      },
      async (pending) => {
        const broker = checked(await waitOf(() => this.cluster.findConnectedBroker()))
        const result = checked(
          await waitOf(() =>
            broker.alterClientQuotas({
              entries: pending.map((item) => wire.get(keyOf(item))),
              validateOnly,
            })
          )
        )
        return result.entries
      }
    )
  }
}

/** Creates the quota family registered inside the single public deadline wrapper.
 * @param {object} options cluster and retry
 * @returns {object} bound public methods
 */
module.exports = (options) => {
  const family = new ClientQuotas(options)
  return {
    describeClientQuotas: family.describeClientQuotas.bind(family),
    alterClientQuotas: family.alterClientQuotas.bind(family),
  }
}
