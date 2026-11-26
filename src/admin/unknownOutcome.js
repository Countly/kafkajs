const apiKeys = require('../protocol/requests/apiKeys')

/** APIs whose admin invocation may write at most one frame. */
const SINGLE_SEND = new Set([
  apiKeys.DeleteTopics,
  apiKeys.OffsetDelete,
  apiKeys.LeaveGroup,
  apiKeys.AlterShareGroupOffsets,
  apiKeys.DeleteShareGroupOffsets,
  apiKeys.WriteTxnMarkers,
  apiKeys.CreateAcls,
  apiKeys.DeleteAcls,
  apiKeys.AlterPartitionReassignments,
  apiKeys.ElectLeaders,
  apiKeys.AlterClientQuotas,
  apiKeys.AlterUserScramCredentials,
  apiKeys.UpdateFeatures,
  apiKeys.CreateDelegationToken,
  apiKeys.RenewDelegationToken,
  apiKeys.ExpireDelegationToken,
  apiKeys.UnregisterController,
])

/** @throws {TypeError} when a request item cannot be named */
const malformed = () => {
  throw new TypeError('Malformed request identity')
}

/** @param {unknown} value @returns {string} valid wire string */
const text = (value) => (typeof value === 'string' && value.isWellFormed() ? value : malformed())

/** @param {unknown} value @param {number} limit @returns {number} signed wire integer */
const integer = (value, limit = 2 ** 31) =>
  Number.isInteger(value) && value >= -limit && value < limit ? value : malformed()

/** @param {object} value @returns {object} plain identity entry */
const entry = (value) => {
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null ? value : malformed()
}

/** @param {object} resource @returns {{type:number,name:string}} config resource identity */
const config = (resource) => {
  const { type, name } = entry(resource)
  return { type: integer(type, 128), name: text(name) }
}

/** @param {object[]} topics @param {Function} cells @returns {object[]} topic partition identities */
const byPartition = (topics, cells) =>
  topics.flatMap((topic) => {
    const name = text(entry(topic).topic)
    return topic.partitions.map((partition) => ({ topic: name, ...cells(partition) }))
  })

const ITEMS = {
  [apiKeys.InitProducerId]: ({ transactionalId }) => [text(transactionalId)],
  [apiKeys.DeleteGroups]: (groups) => groups.map(text),
  [apiKeys.DeleteRecords]: ({ topics }) =>
    byPartition(topics, (partition) => ({ partition: integer(entry(partition).partition) })),
  [apiKeys.AlterConfigs]: ({ resources }) => resources.map(config),
  [apiKeys.IncrementalAlterConfigs]: ({ resources }) => resources.map(config),
  [apiKeys.AlterReplicaLogDirs]: ({ dirs }, nodeId) =>
    dirs.flatMap((dir) =>
      byPartition(entry(dir).topics, (partition) => ({
        brokerId: integer(nodeId),
        partition: integer(partition),
      })).map(({ topic, brokerId, partition }) => ({ brokerId, topic, partition }))
    ),
}

const MUTATING_APIS = new Set([...SINGLE_SEND, ...Object.keys(ITEMS).map(Number)])
const VALIDATE_ONLY_SINCE = {
  [apiKeys.AlterConfigs]: 0,
  [apiKeys.IncrementalAlterConfigs]: 0,
  [apiKeys.AlterClientQuotas]: 0,
  [apiKeys.UpdateFeatures]: 1,
}

/**
 * @param {number} apiKey @param {number} version negotiated wire version
 * @param {object} args request arguments @returns {boolean} whether this request can mutate state
 */
const isMutation = (apiKey, version, args) =>
  MUTATING_APIS.has(apiKey) &&
  !(version >= VALIDATE_ONLY_SINCE[apiKey] && args?.validateOnly === true)

/** @param {object} request built request @param {number} nodeId target broker
 * @returns {object[]|string[]|null} identities, or null if they are absent or cannot be named
 */
const itemsOf = ({ request, args }, nodeId) => {
  try {
    return ITEMS[request.apiKey]?.(args, nodeId) ?? null
  } catch {
    return null
  }
}

/** @param {object|string} item an identity read from the built request @returns {string} key */
const keyOf = (item) => JSON.stringify(item)

/**
 * Codes a broker answers before it applies or schedules anything for the refused items: the
 * controller mutation quota for DeleteTopics, and a group coordinator still loading for
 * OffsetDelete and LeaveGroup. Only these answers let a call send the refused items again.
 */
const REFUSED_BEFORE_APPLYING = new Map([
  [apiKeys.DeleteTopics, 89],
  [apiKeys.OffsetDelete, 14],
  [apiKeys.LeaveGroup, 14],
])

/** @param {unknown[]} values @returns {boolean} whether no value repeats */
const distinct = (values) => new Set(values).size === values.length

/**
 * What a decoded answer lets the same call send again. A DeleteTopics refusal is bound to the
 * topic ids the answer gives, never to names, so a topic recreated under a refused name is not
 * reached; an answer whose rows lack an id, or repeat an id or a name, permits nothing.
 * @param {number} apiKey @param {object} decoded answer
 * @returns {Set<string>|true|null} the refused topic ids of a DeleteTopics answer whose every
 *   failed topic was refused by the quota, true for a whole request the coordinator refused
 *   while loading, or null when nothing may be sent again
 */
const resendable = (apiKey, decoded) => {
  const code = REFUSED_BEFORE_APPLYING.get(apiKey)
  if (code === undefined || !decoded) return null
  if (apiKey !== apiKeys.DeleteTopics) return decoded.errorCode === code ? true : null
  const rows = decoded.topicErrors ?? []
  const failed = rows.filter(({ errorCode }) => errorCode !== 0)
  if (
    failed.length === 0 ||
    failed.some(({ errorCode, topicId }) => errorCode !== code || typeof topicId !== 'string') ||
    !distinct(rows.map(({ topicId }) => topicId).filter((id) => id != null)) ||
    !distinct(rows.map(({ topic }) => topic).filter((name) => name != null))
  )
    return null
  return new Set(failed.map(({ topicId }) => topicId))
}

/**
 * @param {Set<string>|true|null} refused what an earlier answer let this call send again
 * @param {object} args the new request's arguments
 * @returns {boolean} whether the new request carries only items that answer refused: for
 *   DeleteTopics, a nonempty list of refused topic ids and no names
 */
const mayResend = (refused, args) => {
  if (refused === true) return true
  if (!(refused instanceof Set)) return false
  const ids = args.topicIds
  return Array.isArray(ids) && ids.length > 0 && ids.every((id) => refused.has(id))
}

/**
 * Copies plain request data, reading each property once and preserving shared references.
 * Other values are passed to the encoder as supplied.
 * @param {any} value @param {Map<object, object>} [copies] copies already made
 * @returns {any} private input
 */
const owned = (value, copies = new Map()) => {
  if (value === null || typeof value !== 'object') return value
  if (copies.has(value)) return copies.get(value)
  if (Array.isArray(value)) {
    const copy = new Array(value.length)
    copies.set(value, copy)
    for (let index = 0; index < value.length; index++) copy[index] = owned(value[index], copies)
    return copy
  }
  const prototype = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) return value
  const copy = Object.create(prototype)
  copies.set(value, copy)
  for (const key of Object.keys(value)) {
    Object.defineProperty(copy, key, {
      value: owned(value[key], copies),
      enumerable: true,
      writable: true,
      configurable: true,
    })
  }
  return copy
}

module.exports = {
  SINGLE_SEND,
  MUTATING_APIS,
  REFUSED_BEFORE_APPLYING,
  isMutation,
  itemsOf,
  keyOf,
  owned,
  resendable,
  mayResend,
}
