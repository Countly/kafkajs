const apiKeys = require('../protocol/requests/apiKeys')

/** @returns {never} @throws {TypeError} the items of the request cannot be named */
const malformed = () => {
  throw new TypeError('Malformed request identity')
}
/** @param {unknown} value @returns {string} @throws {TypeError} unless a string */
const text = (value) => (typeof value === 'string' ? value : malformed())
/** @param {unknown} value @returns {number} @throws {TypeError} unless an integer */
const int = (value) => (Number.isInteger(value) ? value : malformed())
/** @param {unknown} value @returns {any[]} @throws {TypeError} unless an array */
const list = (value) => (Array.isArray(value) ? value : malformed())

/**
 * The `{ topic, partition }` cells of a topic list.
 * @param {object[]} topics entries with a topic name and a partition list
 * @param {(entry: object) => unknown} nameOf the topic name of an entry
 * @param {(entry: object) => unknown} partitionsOf the partition list of an entry
 * @param {(partition: any) => unknown} partitionOf the partition number of a list element
 * @returns {{ topic: string, partition: number }[]}
 * @throws {TypeError} for a missing or malformed name, list or number
 */
const cells = (topics, nameOf, partitionsOf, partitionOf) =>
  list(topics).flatMap((entry) =>
    list(partitionsOf(entry)).map((partition) => ({
      topic: text(nameOf(entry)),
      partition: int(partitionOf(partition)),
    }))
  )
const topicOf = (entry) => entry.topic
const partitionsOf = (entry) => entry.partitions
const partitionOf = (partition) => partition.partition
const itself = (partition) => partition

/**
 * Adds the request's group to each cell: an offset belongs to a group, not only to a partition.
 * @param {unknown} groupId
 * @param {{ topic: string, partition: number }[]} topicCells
 * @returns {{ groupId: string, topic: string, partition: number }[]}
 * @throws {TypeError} unless the group is a string
 */
const grouped = (groupId, topicCells) =>
  topicCells.map(({ topic, partition }) => ({ groupId: text(groupId), topic, partition }))

/** @param {{ entityType: string, entityName: string|null }} a @param {typeof a} b @returns {number} */
const byEntityType = (a, b) =>
  a.entityType < b.entityType ? -1 : a.entityType > b.entityType ? 1 : 0

/**
 * The items each request that changes broker state carries, read from the input it was built from
 * (the same private copy its encoder reads). `null` means the items cannot be named: such a request
 * is compared with every earlier request of its API. A missing or malformed identity field makes
 * the items unknown rather than partly named. Secrets (token HMACs, SCRAM salts and passwords) are
 * never items.
 * @type {{ [apiKey: number]: (args: any, nodeId: number) => any[]|null }}
 */
const ITEMS = {
  [apiKeys.OffsetCommit]: ({ groupId, topics }) =>
    grouped(groupId, cells(topics, topicOf, partitionsOf, partitionOf)),
  [apiKeys.LeaveGroup]: ({ groupId, memberId, groupInstanceId = null, members }) =>
    (members === undefined ? [{ memberId, groupInstanceId }] : list(members)).map((member) => ({
      groupId: text(groupId),
      memberId: text(member.memberId),
      groupInstanceId: member.groupInstanceId == null ? null : text(member.groupInstanceId),
    })),
  [apiKeys.CreateTopics]: ({ topics }) => list(topics).map((entry) => text(entry.topic)),
  [apiKeys.DeleteTopics]: ({ topics, topicIds }) => list(topicIds ?? topics).map(text),
  [apiKeys.DeleteRecords]: ({ topics }) => cells(topics, topicOf, partitionsOf, partitionOf),
  [apiKeys.InitProducerId]: ({ transactionalId }) => [text(transactionalId)],
  [apiKeys.WriteTxnMarkers]: ({ markers }) =>
    list(markers).flatMap(({ producerId, topics }) => {
      const id = producerId == null ? malformed() : String(producerId)
      return cells(topics, topicOf, partitionsOf, itself).map((cell) => ({
        ...cell,
        producerId: id,
      }))
    }),
  [apiKeys.CreateAcls]: () => null,
  [apiKeys.DeleteAcls]: () => null,
  [apiKeys.AlterConfigs]: ({ resources }) =>
    list(resources).map(({ type, name }) => ({ type: int(type), name: text(name) })),
  // A replica's log directory belongs to one broker: the same partition on another broker is
  // another item.
  [apiKeys.AlterReplicaLogDirs]: ({ dirs }, nodeId) =>
    list(dirs)
      .flatMap((dir) => cells(dir.topics, topicOf, partitionsOf, itself))
      .map((cell) => ({ brokerId: nodeId, ...cell })),
  [apiKeys.CreatePartitions]: ({ topicPartitions }) =>
    list(topicPartitions).map((entry) => text(entry.topic)),
  [apiKeys.CreateDelegationToken]: () => null,
  [apiKeys.RenewDelegationToken]: () => null,
  [apiKeys.ExpireDelegationToken]: () => null,
  [apiKeys.DeleteGroups]: (groupIds) => list(groupIds).map(text),
  [apiKeys.ElectLeaders]: ({ topicPartitions }) =>
    topicPartitions == null || list(topicPartitions).some(({ partitions }) => partitions == null)
      ? null
      : cells(topicPartitions, topicOf, partitionsOf, itself),
  [apiKeys.IncrementalAlterConfigs]: ({ resources }) =>
    list(resources).map(({ type, name }) => ({ type: int(type), name: text(name) })),
  [apiKeys.AlterPartitionReassignments]: ({ topics }) =>
    cells(topics, topicOf, (entry) => entry.partitionAssignment, partitionOf),
  [apiKeys.OffsetDelete]: ({ groupId, topics }) =>
    grouped(groupId, cells(topics, topicOf, partitionsOf, partitionOf)),
  // An entity is a set of (type, name) parts: the order a caller listed them in does not matter.
  [apiKeys.AlterClientQuotas]: ({ entries }) =>
    list(entries).map(({ entity }) =>
      list(entity)
        .map(({ entityType, entityName }) => ({
          entityType: text(entityType),
          entityName: entityName === null ? null : text(entityName),
        }))
        .sort(byEntityType)
    ),
  [apiKeys.AlterUserScramCredentials]: ({ deletions, upsertions }) => [
    ...new Set([...list(deletions), ...list(upsertions)].map(({ name }) => text(name))),
  ],
  [apiKeys.UpdateFeatures]: ({ featureUpdates }) =>
    list(featureUpdates).map(({ feature }) => text(feature)),
  [apiKeys.AlterShareGroupOffsets]: ({ groupId, topics }) =>
    grouped(
      groupId,
      cells(
        topics,
        (entry) => entry.topicName,
        partitionsOf,
        (partition) => partition.partitionIndex
      )
    ),
  [apiKeys.DeleteShareGroupOffsets]: ({ groupId, topics }) =>
    list(topics).map((entry) => ({ groupId: text(groupId), topic: text(entry.topicName) })),
  [apiKeys.UnregisterController]: ({ controllerId }) => [int(controllerId)],
}

/** The API keys of requests that change broker state. */
const MUTATING_APIS = new Set(Object.keys(ITEMS).map(Number))

/** The first version at which each API's request carries `validateOnly` on the wire. */
const VALIDATE_ONLY_SINCE = {
  [apiKeys.CreateTopics]: 0,
  [apiKeys.CreatePartitions]: 0,
  [apiKeys.AlterConfigs]: 0,
  [apiKeys.IncrementalAlterConfigs]: 0,
  [apiKeys.AlterClientQuotas]: 0,
  [apiKeys.UpdateFeatures]: 1,
}

/** @param {object[]} items the items of an offset request @returns {string|undefined} its group */
const groupOf = (items) => items[0]?.groupId

/**
 * The answers that definitely refused an item: the broker did not apply it, so sending exactly that
 * item again is safe. `top` lists the codes of the response's top-level error that refuse the whole
 * request; `rows` the codes of a per-item row that refuse that item, and `read` maps the decoded
 * body to one `{ item, errorCode }` per row, each item in the shape `ITEMS` gives it. An API that
 * is not here gets no resend once written. Code 7 is on no list.
 * @type {{ [apiKey: number]: { top?: number[], rows?: number[],
 *   read?: (decoded: any, items: any[]) => { item: any, errorCode: number }[] } }}
 */
const REFUSALS = {
  [apiKeys.InitProducerId]: { top: [14, 15, 16, 51] },
  [apiKeys.OffsetCommit]: {
    rows: [14, 15, 16, 27],
    read: (decoded, items) =>
      list(decoded.responses).flatMap(({ topic, partitions }) =>
        list(partitions).map(({ partition, errorCode }) => ({
          item: { groupId: groupOf(items), topic, partition },
          errorCode,
        }))
      ),
  },
  [apiKeys.OffsetDelete]: {
    top: [14, 15, 16],
    rows: [14, 15, 16, 27],
    read: (decoded, items) =>
      list(decoded.topics).flatMap(({ topic, partitions }) =>
        list(partitions).map(({ partition, errorCode }) => ({
          item: { groupId: groupOf(items), topic, partition },
          errorCode,
        }))
      ),
  },
  [apiKeys.AlterShareGroupOffsets]: { top: [14, 15, 16] },
  [apiKeys.DeleteShareGroupOffsets]: { top: [14, 15, 16] },
  [apiKeys.WriteTxnMarkers]: {
    rows: [6],
    read: (decoded) =>
      list(decoded.markers).flatMap(({ producerId, topics }) =>
        list(topics).flatMap(({ topic, partitions }) =>
          list(partitions).map(({ partition, errorCode }) => ({
            item: { topic, partition, producerId },
            errorCode,
          }))
        )
      ),
  },
  [apiKeys.ElectLeaders]: { top: [41] },
  [apiKeys.UpdateFeatures]: { top: [41] },
  [apiKeys.AlterUserScramCredentials]: {
    rows: [41],
    read: (decoded) =>
      list(decoded.results).map(({ user, errorCode }) => ({ item: user, errorCode })),
  },
  [apiKeys.LeaveGroup]: { top: [14, 15, 16] },
  [apiKeys.UnregisterController]: { top: [41] },
  [apiKeys.CreateDelegationToken]: { top: [41] },
  [apiKeys.RenewDelegationToken]: { top: [41] },
  [apiKeys.ExpireDelegationToken]: { top: [41] },
  [apiKeys.CreateTopics]: {
    rows: [41, 89],
    read: (decoded) =>
      list(decoded.topicErrors).map(({ topic, errorCode }) => ({ item: topic, errorCode })),
  },
  [apiKeys.CreatePartitions]: {
    rows: [41, 89],
    read: (decoded) =>
      list(decoded.topicErrors).map(({ topic, errorCode }) => ({ item: topic, errorCode })),
  },
}

/** @param {any} item @returns {string} the identity an item is compared by */
const keyOf = (item) => JSON.stringify(item)

/**
 * The items one decoded answer definitely refused (`REFUSALS`). Per-item codes count only when the
 * answer has exactly one row for each item of the request and no other row; the whole body is
 * checked before anything is returned, and a body that cannot be read refuses nothing.
 * @param {number} apiKey the request's API
 * @param {any} decoded the decoded answer to that request
 * @param {any[]|null} items the request's items (`itemsOf`)
 * @returns {true|string[]} `true` when the whole request was refused, else the keys (`keyOf`) of
 *   the refused items, possibly none
 */
const definiteRefusals = (apiKey, decoded, items) => {
  const rule = REFUSALS[apiKey]
  if (!rule) return []
  try {
    if (rule.top) {
      const code = decoded.errorCode
      if (rule.top.includes(code)) return true
      if (!rule.rows || code !== 0) return []
    }
    if (items === null) return []
    const wanted = new Set(items.map(keyOf))
    const rows = rule.read(decoded, items)
    if (rows.length !== wanted.size) return []
    const seen = new Set()
    const refused = []
    for (const { item, errorCode } of rows) {
      const key = keyOf(item)
      if (!wanted.has(key) || seen.has(key) || !Number.isInteger(errorCode)) return []
      seen.add(key)
      if (rule.rows.includes(errorCode)) refused.push(key)
    }
    return refused
  } catch {
    return []
  }
}

/**
 * Whether a built request changes broker state: its API is in `MUTATING_APIS`, and it is not a
 * request whose wire `validateOnly` flag is set (the encoder writes any truthy value as true).
 * @param {{ request: { apiKey: number, apiVersion: number }, args?: object }} protocolRequest
 * @returns {boolean}
 */
const isMutation = ({ request, args }) => {
  if (!MUTATING_APIS.has(request.apiKey)) return false
  const since = VALIDATE_ONLY_SINCE[request.apiKey]
  return !(since !== undefined && request.apiVersion >= since && args?.validateOnly)
}

/**
 * The items a mutating request carries.
 * @param {{ request: { apiKey: number }, args?: object }} protocolRequest a built request
 * @param {number} nodeId the broker it goes to
 * @returns {any[]|null} the items, or `null` when they cannot be named from its input
 */
const itemsOf = ({ request, args }, nodeId) => {
  try {
    const items = args === undefined ? null : ITEMS[request.apiKey](args, nodeId)
    return Array.isArray(items) ? items : null
  } catch {
    return null
  }
}

/**
 * A private copy of a request's input, taken when the request is built, so that the encoder and
 * the replay check read the same values whatever the caller changes later. Arrays and plain objects
 * are copied (a property is read once); every other value (strings, numbers, Buffers, Long) is kept
 * as it is. Shared and cyclic references stay shared and cyclic.
 * @param {any} value the input
 * @param {Map<object, object>} [copies] copies already made
 * @returns {any}
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

module.exports = { MUTATING_APIS, isMutation, itemsOf, keyOf, definiteRefusals, owned }
