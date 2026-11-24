const apiKeys = require('../protocol/requests/apiKeys')

/**
 * The `{ topic, partition }` cells of a topic list.
 * @param {object[]} topics entries with a topic name and a partition list
 * @param {(topic: object) => any[]} partitionsOf the partition list of an entry
 * @param {(partition: any) => number} [partitionOf] the partition number of a list element
 * @returns {{ topic: string, partition: number }[]}
 */
const cells = (topics, partitionsOf, partitionOf = (partition) => partition.partition) =>
  topics.flatMap((entry) =>
    partitionsOf(entry).map((partition) => ({
      topic: entry.topic,
      partition: partitionOf(partition),
    }))
  )

/**
 * The cells of a topic list whose `partitions` are plain partition numbers.
 * @param {{ topic: string, partitions: number[] }[]} topics
 * @returns {{ topic: string, partition: number }[]}
 */
const numberedCells = (topics) =>
  cells(
    topics,
    ({ partitions }) => partitions,
    (partition) => partition
  )

/**
 * The items each request that changes broker state carries, read from the arguments its builder
 * was given. `null` means the items cannot be named: such a request is compared with every
 * earlier request of its API. Secrets (token HMACs, SCRAM salts and passwords) are never items.
 * @type {{ [apiKey: number]: (args: object) => any[]|null }}
 */
const ITEMS = {
  [apiKeys.OffsetCommit]: ({ topics }) => cells(topics, ({ partitions }) => partitions),
  [apiKeys.LeaveGroup]: ({ groupId, memberId, groupInstanceId, members }) =>
    (members ?? [{ memberId, groupInstanceId }]).map((member) => ({
      groupId,
      memberId: member.memberId ?? null,
      groupInstanceId: member.groupInstanceId ?? null,
    })),
  [apiKeys.CreateTopics]: ({ topics }) => topics.map(({ topic }) => topic),
  [apiKeys.DeleteTopics]: ({ topics, topicIds }) => [...(topicIds ?? topics)],
  [apiKeys.DeleteRecords]: ({ topics }) => cells(topics, ({ partitions }) => partitions),
  [apiKeys.InitProducerId]: ({ transactionalId }) => [transactionalId],
  [apiKeys.WriteTxnMarkers]: ({ markers }) =>
    markers.flatMap(({ producerId, topics }) =>
      numberedCells(topics).map((cell) => ({
        ...cell,
        producerId: String(producerId),
      }))
    ),
  [apiKeys.CreateAcls]: () => null,
  [apiKeys.DeleteAcls]: () => null,
  [apiKeys.AlterConfigs]: ({ resources }) => resources.map(({ type, name }) => ({ type, name })),
  [apiKeys.AlterReplicaLogDirs]: ({ dirs }) => dirs.flatMap(({ topics }) => numberedCells(topics)),
  [apiKeys.CreatePartitions]: ({ topicPartitions }) => topicPartitions.map(({ topic }) => topic),
  [apiKeys.CreateDelegationToken]: () => null,
  [apiKeys.RenewDelegationToken]: () => null,
  [apiKeys.ExpireDelegationToken]: () => null,
  [apiKeys.DeleteGroups]: (groupIds) => [...groupIds],
  [apiKeys.ElectLeaders]: ({ topicPartitions }) =>
    topicPartitions == null || topicPartitions.some(({ partitions }) => partitions == null)
      ? null
      : numberedCells(topicPartitions),
  [apiKeys.IncrementalAlterConfigs]: ({ resources }) =>
    resources.map(({ type, name }) => ({ type, name })),
  [apiKeys.AlterPartitionReassignments]: ({ topics }) =>
    cells(topics, ({ partitionAssignment }) => partitionAssignment),
  [apiKeys.OffsetDelete]: ({ topics }) => cells(topics, ({ partitions }) => partitions),
  [apiKeys.AlterClientQuotas]: ({ entries }) =>
    entries.map(({ entity }) =>
      entity.map(({ entityType, entityName }) => ({ entityType, entityName }))
    ),
  [apiKeys.AlterUserScramCredentials]: ({ deletions, upsertions }) =>
    [...deletions, ...upsertions].map(({ name }) => name),
  [apiKeys.UpdateFeatures]: ({ featureUpdates }) => featureUpdates.map(({ feature }) => feature),
  [apiKeys.AlterShareGroupOffsets]: ({ groupId, topics }) =>
    cells(
      topics,
      ({ partitions }) => partitions,
      ({ partitionIndex }) => partitionIndex
    ).map(({ topic, partition }) => ({ groupId, topic, partition })),
  [apiKeys.DeleteShareGroupOffsets]: ({ groupId, topics }) =>
    topics.map(({ topicName }) => ({ groupId, topic: topicName })),
  [apiKeys.UnregisterController]: ({ controllerId }) => [controllerId],
}

/** The API keys of requests that change broker state. */
const MUTATING_APIS = new Set(Object.keys(ITEMS).map(Number))

/**
 * The answers that definitely refused an item, by API: from one decoded body, the items of the
 * request it refused with a code that makes a resend of exactly those items safe. Empty: no
 * answer permits a resend yet.
 * @type {{ [apiKey: number]: (decoded: object, items: any[]) => any[] }}
 */
const DEFINITE_REFUSALS = {}

/**
 * Whether a built request changes broker state: its API is in `MUTATING_APIS` and it is not a
 * `validateOnly` request.
 * @param {{ request: { apiKey: number }, args?: object }} protocolRequest a built request
 * @returns {boolean}
 */
const isMutation = ({ request, args }) =>
  MUTATING_APIS.has(request.apiKey) && args?.validateOnly !== true

/**
 * The items a mutating request carries.
 * @param {{ request: { apiKey: number }, args?: object }} protocolRequest a built request
 * @returns {any[]|null} the items, or `null` when they cannot be read from its arguments
 */
const itemsOf = ({ request, args }) => {
  try {
    return args === undefined ? null : ITEMS[request.apiKey](args)
  } catch {
    return null
  }
}

module.exports = { MUTATING_APIS, DEFINITE_REFUSALS, isMutation, itemsOf }
