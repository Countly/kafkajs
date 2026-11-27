const describeThrown = require('../network/describeThrown')
const createDelegationTokens = require('./delegationTokens')
const createClientQuotas = require('./clientQuotas')
const createScramCredentials = require('./scramCredentials')
const createTransactions = require('./transactions')
const createClusterAdmin = require('./clusterAdmin')
const createConfigResources = require('./configResources')
const createUnregisterController = require('./unregisterController')
const { inspect } = require('util')
const AdminLifecycle = require('./lifecycle')
const {
  retrierOf,
  endsCall,
  isAnsweredError,
  settleMutations,
  wrapAdmin,
  validateApiTimeout,
  checked,
  waitOf,
  sharedSetup,
  waitForOf: waitFor,
} = require('./deadline')
const { createOffsets, OffsetAdmin } = require('./offsets')
const createLogDirs = require('./logDirs')
const { ADMIN_MUTATION } = require('../broker/privateOptions')
const groupBy = require('../utils/groupBy')
const Long = require('../utils/long')
const { assertPartitionId, isPartitionId, printable } = require('../utils/partitionId')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const { events, wrap: wrapEvent, unwrap: unwrapEvent } = require('./instrumentationEvents')
const {
  KafkaJSNonRetriableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSDeleteGroupsError,
  KafkaJSBrokerNotFound,
  KafkaJSDeleteTopicRecordsError,
  isStaleCoordinator,
  COORDINATOR_LOAD_IN_PROGRESS_CODE,
} = require('../errors')
const { staleMetadata, staleLeaderEpoch, createErrorFromCode } = require('../protocol/error')
const { GROUP_TYPES } = require('../protocol/groupTypes')
const Encoder = require('../protocol/encoder')
const { createGroupDescriptions } = require('./groupDescriptions')
const { createShareGroups } = require('./shareGroups')
const { AggregateOutcome, isPerItemFailure } = require('./aggregateOutcome')
const {
  readTopicPages,
  toTopicsMetadata,
  withReadDeadline,
  checkDeadline,
} = require('./describeTopicsByPartition')
const CONFIG_RESOURCE_TYPES = require('../protocol/configResourceTypes')
const ALTER_CONFIG_OP_TYPES = require('../protocol/alterConfigOpTypes')
const ACL_RESOURCE_TYPES = require('../protocol/aclResourceTypes')
const ACL_OPERATION_TYPES = require('../protocol/aclOperationTypes')
const ACL_PERMISSION_TYPES = require('../protocol/aclPermissionTypes')
const RESOURCE_PATTERN_TYPES = require('../protocol/resourcePatternTypes')
const { EARLIEST_OFFSET, LATEST_OFFSET } = require('../constants')

/** Maps the public isolation option without changing shared cluster defaults.
 * @param {object} [options] invocation options, including its inherited API budget
 * @returns {number} the ListOffsets isolation byte
 * @throws {KafkaJSInvalidConfigurationError} for an invalid new option
 */
const isolationOf = (options = {}) => {
  if (options === null || typeof options !== 'object' || Array.isArray(options)) {
    throw new KafkaJSInvalidConfigurationError('isolationLevel options must be an object')
  }
  const { isolationLevel = 'read_committed' } = options
  if (!['read_committed', 'read_uncommitted'].includes(isolationLevel)) {
    throw new KafkaJSInvalidConfigurationError(
      'isolationLevel must be read_committed or read_uncommitted'
    )
  }
  return isolationLevel === 'read_committed' ? 1 : 0
}

const { CONNECT, DISCONNECT } = events

const NOT_CONTROLLER_CODE = 41
const GROUP_ID_NOT_FOUND_CODE = 69
const UNKNOWN_TOPIC_OR_PARTITION_CODE = 3
/** The largest offset a commit can carry: OffsetCommit offsets are signed int64. */
const MAX_OFFSET = 2n ** 63n - 1n
const CONSUMER_PROTOCOL_TYPE = 'consumer'
const UNANSWERED_MEMBER_CODE = -1
const THROTTLING_QUOTA_EXCEEDED_CODE = 89
/**
 * Failures a deleteTopics attempt tries again when it wrote nothing: the controller moved, or a
 * target topic is missing from metadata for now. Both the by-name and the by-id attempt use it.
 */
const DELETE_TOPICS_RETRIED = new Set(['NOT_CONTROLLER', 'UNKNOWN_TOPIC_OR_PARTITION'])

/** What the coordinator logs as the reason a member was removed, when the caller gives none. */
const DEFAULT_REMOVE_REASON = 'member was removed by an admin'
/** The longest reason sent; Java's `JoinGroupRequest.maybeTruncateReason` cuts at the same length. */
const MAX_REMOVE_REASON_LENGTH = 255

const { values, keys, entries } = Object
const eventNames = values(events)
const eventKeys = keys(events)
  .map((key) => `admin.events.${key}`)
  .join(', ')

/**
 * Runs `fn` until it resolves with a truthy value, retrying (via {@link waitFor}) on
 * `LEADER_NOT_AVAILABLE` and, when `opts.allowAutoTopicCreation` or `opts.waitForTopicPropagation`
 * is set, also on `UNKNOWN_TOPIC_OR_PARTITION` — KRaft answers the latter while a not-yet-created
 * topic is still being created asynchronously through the controller, and while a topic the
 * controller has just committed has not reached the broker that is asked.
 * Any other error, or the timeout expiring, rejects.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/admin/Admin.java
 * @param {() => Promise<any>} fn
 * @param {Object} [opts]
 * @param {boolean} [opts.allowAutoTopicCreation=false]
 * @param {boolean} [opts.waitForTopicPropagation=false] the topics were created by this call
 * @param {number} [opts.delay]
 * @param {number} [opts.maxWait]
 * @param {string} [opts.timeoutMessage]
 * @returns {Promise<any>}
 * @throws {KafkaJSTimeout} if `fn` keeps failing/returning falsy past `opts.maxWait`
 */
const retryOnLeaderNotAvailable = (fn, opts = {}) => {
  const { allowAutoTopicCreation = false, waitForTopicPropagation = false } = opts
  const callback = async () => {
    try {
      return checked(await waitOf(async () => fn()))
    } catch (e) {
      const isTopicStillAppearing =
        (allowAutoTopicCreation || waitForTopicPropagation) &&
        e.type === 'UNKNOWN_TOPIC_OR_PARTITION'
      if (e.type !== 'LEADER_NOT_AVAILABLE' && !isTopicStillAppearing) {
        throw e
      }
      return false
    }
  }

  return waitFor(callback, opts)
}

const isConsumerGroupRunning = (description) => ['Empty', 'Dead'].includes(description.state)

/**
 * An offset as a commit carries it. Only a decimal integer string (an optional sign and digits), a
 * safe integer number, a bigint or a `Long` is read; anything else, `'NaN'`, a blank or prefixed
 * string, a boolean or an array say, is refused rather than read as 0 the way `Long.fromValue` would.
 * @param {string|number|bigint|Long} offset a long of at least 0, or `-1` (latest) or `-2` (earliest)
 * @returns {string} the offset in decimal
 * @throws {KafkaJSNonRetriableError} for anything else, a value above the signed int64 range
 *   included
 */
const commitOffsetOf = (offset) => {
  const text = typeof offset === 'bigint' || offset instanceof Long ? offset.toString() : offset
  const value =
    (typeof text === 'string' && /^[+-]?\d+$/.test(text)) || Number.isSafeInteger(text)
      ? BigInt(text)
      : null
  if (value === null || value > MAX_OFFSET) {
    throw new KafkaJSNonRetriableError(
      `Invalid offset, expected a long received ${printable(offset)}`
    )
  }
  if (value < 0n && value !== BigInt(EARLIEST_OFFSET) && value !== BigInt(LATEST_OFFSET)) {
    throw new KafkaJSNonRetriableError('Offset must not be a negative number')
  }
  return value.toString()
}

/**
 * The items of an array, its length and each index read once. The array's own iterator is not
 * used, and an index the array does not own (a hole, even one its prototype fills) is refused.
 * @param {unknown[]} list
 * @param {(index: number) => string} holeMessage
 * @returns {unknown[]}
 * @throws {KafkaJSNonRetriableError} with `holeMessage` of the first index the array does not own,
 *   or for a length that is not an array length (which only a proxy can report)
 */
const itemsOf = (list, holeMessage) => {
  const length = list.length
  if (!Number.isInteger(length) || length < 0 || length > 2 ** 32 - 1) {
    throw new KafkaJSNonRetriableError(`Invalid array length ${printable(length)}`)
  }
  const items = []
  for (let i = 0; i < length; i++) {
    if (!Object.hasOwn(list, i)) throw new KafkaJSNonRetriableError(holeMessage(i))
    items.push(list[i])
  }
  return items
}

/**
 * A copy of the partitions `deleteTopicRecords` was asked to delete from, each entry read once, so
 * later changes by the caller cannot reach the request.
 * @param {unknown} partitions
 * @returns {{ partition: number, offset: string }[]} each offset in decimal
 * @throws {KafkaJSNonRetriableError} for anything but a non-empty array of entries with a partition
 *   id listed once and an offset from 0 to 2^63 - 1, or `-1` (the high watermark)
 */
const deletionsOf = (partitions) => {
  if (!Array.isArray(partitions)) throw new KafkaJSNonRetriableError('Invalid partitions')

  const expected = (i) => `Invalid partitions, expected { partition, offset } at index ${i}`
  const items = itemsOf(partitions, expected)
  if (items.length === 0) throw new KafkaJSNonRetriableError('Invalid partitions')
  const copy = []
  const seen = new Set()
  for (let i = 0; i < items.length; i++) {
    const entry = items[i]
    if (entry === null || typeof entry !== 'object') {
      throw new KafkaJSNonRetriableError(expected(i))
    }
    const { partition, offset } = entry
    assertPartitionId(partition)
    if (seen.has(partition)) {
      throw new KafkaJSNonRetriableError(
        `Invalid partitions, partition ${partition} is listed more than once`
      )
    }
    const value = commitOffsetOf(offset)
    if (value === String(EARLIEST_OFFSET)) {
      throw new KafkaJSNonRetriableError('Offset must not be a negative number')
    }
    seen.add(partition)
    copy.push({ partition, offset: value })
  }
  return copy
}

/**
 * A copy of the reassignments `alterPartitionReassignments` was asked for, each entry read once,
 * so later changes by the caller cannot reach the request. Holes and `null` entries are refused,
 * not skipped.
 * @param {unknown[]} topics
 * @returns {{ topic: string, partitionAssignment: { partition: number, replicas: number[] }[] }[]}
 * @throws {KafkaJSNonRetriableError} for an entry that is not an object, a topic name that is not
 *   a string or is listed twice, a partition or replica id that is not an integer from 0 to
 *   2^31 - 1, a partition listed twice for a topic, replicas that are not an array, or a replica
 *   listed twice for a partition
 */
const reassignmentsOf = (topics) => {
  const expected = (i) =>
    `Invalid topics array, expected { topic, partitionAssignment } at index ${i}`
  const items = itemsOf(topics, expected)
  const copy = []
  for (let i = 0; i < items.length; i++) {
    const entry = items[i]
    if (entry === null || typeof entry !== 'object') {
      throw new KafkaJSNonRetriableError(expected(i))
    }
    const { topic, partitionAssignment } = entry
    if (typeof topic !== 'string') {
      throw new KafkaJSNonRetriableError(
        'Invalid topics array, the topic names have to be a valid string'
      )
    }
    copy.push({ topic, partitionAssignment })
  }

  if (new Set(copy.map(({ topic }) => topic)).size < copy.length) {
    throw new KafkaJSNonRetriableError(
      'Invalid topics array, it cannot have multiple entries for the same topic'
    )
  }

  for (const entry of copy) {
    const { topic, partitionAssignment } = entry
    if (!Array.isArray(partitionAssignment)) {
      throw new KafkaJSNonRetriableError(
        `Invalid partitions array: ${printable(partitionAssignment)} for topic: ${topic}`
      )
    }

    const expected = (i) =>
      `Invalid partitions array, expected { partition, replicas } at index ${i} for topic: ${topic}`
    const assignments = itemsOf(partitionAssignment, expected)
    const partitions = new Set()
    entry.partitionAssignment = []
    for (let i = 0; i < assignments.length; i++) {
      const assignment = assignments[i]
      if (assignment === null || typeof assignment !== 'object') {
        throw new KafkaJSNonRetriableError(expected(i))
      }
      const { partition, replicas } = assignment
      if (!isPartitionId(partition)) {
        throw new KafkaJSNonRetriableError(
          `Invalid partitions index: ${printable(partition)} for topic: ${topic}`
        )
      }
      if (partitions.has(partition)) {
        throw new KafkaJSNonRetriableError(
          `Invalid partitions array, partition ${partition} is listed more than once for topic: ${topic}`
        )
      }
      partitions.add(partition)

      if (!Array.isArray(replicas)) {
        throw new KafkaJSNonRetriableError(
          `Invalid replica assignment: ${printable(replicas)} for topic: ${topic} on partition: ${partition}`
        )
      }
      const where = `for topic: ${topic} on partition: ${partition}`
      const replicaIds = itemsOf(
        replicas,
        (i) => `Invalid replica assignment: hole at index ${i} ${where}`
      )
      if (!replicaIds.every(isPartitionId)) {
        throw new KafkaJSNonRetriableError(
          `Invalid replica assignment: ${replicaIds.map(printable).join(',')} ${where}. Replicas must be a non negative number`
        )
      }
      const unique = new Set()
      for (const id of replicaIds) {
        if (unique.has(id)) {
          throw new KafkaJSNonRetriableError(
            `Invalid replica assignment: replica ${id} is listed more than once ${where}`
          )
        }
        unique.add(id)
      }
      entry.partitionAssignment.push({ partition, replicas: replicaIds })
    }
  }
  return copy
}

/**
 * Reads and checks the `deleteTopicRecords` options when the call is admitted, before any await.
 * @param {{ topic: string, partitions: unknown }} options
 * @returns {[{ topic: string, partitions: { partition: number, offset: string }[] }]} the
 *   arguments the method runs with
 * @throws {KafkaJSNonRetriableError} for an invalid topic, or partitions `deletionsOf` refuses
 */
const deletionInput = ({ topic, partitions }) => {
  if (!topic || typeof topic !== 'string') {
    throw new KafkaJSNonRetriableError(`Invalid topic "${topic}"`)
  }
  return [{ topic, partitions: deletionsOf(partitions) }]
}

/**
 * Reads and checks the `alterPartitionReassignments` options when the call is admitted, before any
 * await.
 * @param {{ topics: unknown, timeout?: number, allowReplicationFactorChange?: boolean }} options
 * @returns {[{ topics: object[], timeout?: number, allowReplicationFactorChange: boolean }]} the
 *   arguments the method runs with
 * @throws {KafkaJSInvalidConfigurationError} when `allowReplicationFactorChange` is not a boolean
 * @throws {KafkaJSNonRetriableError} for topics that are not an array or `reassignmentsOf` refuses
 */
const reassignmentInput = ({ topics, timeout, allowReplicationFactorChange = true }) => {
  if (typeof allowReplicationFactorChange !== 'boolean')
    throw new KafkaJSInvalidConfigurationError('Invalid allowReplicationFactorChange')
  if (!topics || !Array.isArray(topics)) {
    throw new KafkaJSNonRetriableError(`Invalid topics array ${printable(topics)}`)
  }
  return [{ topics: reassignmentsOf(topics), timeout, allowReplicationFactorChange }]
}

/**
 * The static members `removeMembersFromConsumerGroup` was asked to remove, once each, as LeaveGroup
 * names them: an empty member id and the instance id.
 * @param {{ groupInstanceId: string }[]} members
 * @returns {{ memberId: string, groupInstanceId: string }[]}
 * @throws {KafkaJSNonRetriableError} for an empty list or a blank instance id
 */
const staticMembers = (members) => {
  if (!Array.isArray(members) || members.length === 0) {
    throw new KafkaJSNonRetriableError(
      'Invalid members, expected a non-empty array of { groupInstanceId }'
    )
  }

  const instanceIds = new Set()
  for (const member of members) {
    const instanceId = member == null ? undefined : member.groupInstanceId

    if (typeof instanceId !== 'string' || instanceId.trim() === '') {
      throw new KafkaJSNonRetriableError(
        `Invalid groupInstanceId ${JSON.stringify(instanceId)}, expected a non-blank string`
      )
    }

    instanceIds.add(instanceId)
  }

  return [...instanceIds].map((groupInstanceId) => ({ memberId: '', groupInstanceId }))
}

/**
 * One result per member sent. A member the answer leaves out, which a correct coordinator never
 * does, is reported with the unknown-server-error code rather than as removed.
 * @param {{ memberId: string, groupInstanceId?: string }} sent
 * @param {{ memberId: string, groupInstanceId: string|null, errorCode: number }} [answered]
 * @returns {{ memberId: string, groupInstanceId: string|null, errorCode: number, error: Error|null }}
 */
const toRemoveMemberResult = (sent, answered) => {
  const {
    memberId,
    groupInstanceId = null,
    errorCode,
  } = answered || {
    ...sent,
    errorCode: UNANSWERED_MEMBER_CODE,
  }

  return {
    memberId,
    groupInstanceId,
    errorCode,
    error: errorCode === 0 ? null : createErrorFromCode(errorCode),
  }
}

const findTopicPartitions = async (cluster, topic) => {
  checked(await waitOf(async () => cluster.addTargetTopic(topic)))
  checked(await waitOf(async () => cluster.refreshMetadataIfNecessary()))

  return cluster
    .findTopicPartitionMetadata(topic)
    .map(({ partitionId }) => partitionId)
    .sort((a, b) => a - b)
}
const indexByPartition = (array) =>
  array.reduce(
    (obj, { partition, ...props }) => Object.assign(obj, { [partition]: { ...props } }),
    {}
  )

/** @throws {KafkaJSNonRetriableError} unless `value` is an array of strings */
const assertStringArray = (name, value) => {
  if (!Array.isArray(value) || value.some((entry) => typeof entry !== 'string')) {
    throw new KafkaJSNonRetriableError(`Expected ${name} to be an array of strings`)
  }
}

/**
 * The topics a call that changes topics must send again after the broker's quota refused them.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-599%3A+Throttle+Create+Topic%2C+Create+Partition+and+Delete+Topic+Operations
 * @param {Error} error what `Broker#deleteTopics` or `#createPartitions` rejected with: the first
 *   failed topic's error, carrying its `topicErrors`
 * @returns {Set<string>|null} the refused topics, or null unless every topic that failed was
 *   refused with `THROTTLING_QUOTA_EXCEEDED`
 */
const throttledTopics = ({ topicErrors }) =>
  topicErrors?.length > 0 &&
  topicErrors.every(({ errorCode }) => errorCode === THROTTLING_QUOTA_EXCEEDED_CODE)
    ? new Set(topicErrors.map(({ topic }) => topic))
    : null

/**
 * Binds a by-name DeleteTopics quota refusal to the topic ids the answer gave, so a later attempt
 * reaches only the topics the controller refused, never a topic recreated under the same name.
 * @param {Error} error what `Broker#deleteTopics` rejected with, carrying the failed `topicErrors`
 * @param {string[]} names the names the request carried
 * @returns {Map<string, string>|null} refused id to name, or null unless every failed row was
 *   refused by the quota and names one requested topic and its id, each once
 */
const quotaRefusedIds = ({ topicErrors }, names) => {
  const refused = throttledTopics({ topicErrors })
  if (!refused || refused.size !== topicErrors.length) return null
  const requested = new Set(names)
  const ids = new Map()
  for (const { topic, topicId } of topicErrors) {
    if (!requested.has(topic) || typeof topicId !== 'string' || ids.has(topicId)) return null
    ids.set(topicId, topic)
  }
  return ids
}

/**
 * Whether a config resource names something: a non-empty string, except for BROKER, whose empty
 * name stands for the cluster-wide default broker config.
 * @param {{ type: number, name: unknown }} resource
 * @returns {boolean}
 */
const hasValidResourceName = ({ type, name }) =>
  typeof name === 'string' && (name !== '' || type === CONFIG_RESOURCE_TYPES.BROKER)

/**
 * The first check `describeConfigs`, `alterConfigs` and `incrementalAlterConfigs` make on their
 * `resources`.
 * @param {unknown} resources
 * @returns {void}
 * @throws {KafkaJSNonRetriableError} unless `resources` is an array with an entry
 */
const assertResourcesArray = (resources) => {
  if (!resources || !Array.isArray(resources)) {
    throw new KafkaJSNonRetriableError(`Invalid resources array ${resources}`)
  }

  if (resources.length === 0) {
    throw new KafkaJSNonRetriableError('Resources array cannot be empty')
  }
}

/**
 * The second check the three config methods share: every resource has a known type and a name
 * {@link hasValidResourceName} accepts.
 * @param {{ type: number, name: unknown }[]} resources
 * @returns {void}
 * @throws {KafkaJSNonRetriableError} for the first resource that fails, naming it
 */
const assertResourceTypesAndNames = (resources) => {
  const validResourceTypes = Object.values(CONFIG_RESOURCE_TYPES)
  const invalidType = resources.find((r) => !validResourceTypes.includes(r.type))

  if (invalidType) {
    throw new KafkaJSNonRetriableError(
      `Invalid resource type ${invalidType.type}: ${JSON.stringify(invalidType)}`
    )
  }

  const invalidName = resources.find((r) => !hasValidResourceName(r))

  if (invalidName) {
    throw new KafkaJSNonRetriableError(
      `Invalid resource name ${invalidName.name}: ${JSON.stringify(invalidName)}`
    )
  }
}

/**
 * Checks one config entry of an `incrementalAlterConfigs` request.
 * @param {unknown} entry
 * @param {string} resourceKey `<type>/<name>` of the resource that holds it, for the message
 * @returns {void}
 * @throws {KafkaJSNonRetriableError} unless `entry` is an object with a string `name`, an
 *   `operation` that is an `AlterConfigOpTypes` value, and a string `value` unless it is a DELETE,
 *   whose `value` is not looked at
 */
const assertIncrementalEntry = (entry, resourceKey) => {
  if (entry == null || typeof entry !== 'object') {
    throw new KafkaJSNonRetriableError(
      `Invalid config entry ${inspect(entry)} of resource ${resourceKey}: it must be an object`
    )
  }

  const { name, operation, value } = entry

  if (typeof name !== 'string') {
    throw new KafkaJSNonRetriableError(
      `Invalid config entry name ${inspect(name)} of resource ${resourceKey}`
    )
  }

  if (!Object.values(ALTER_CONFIG_OP_TYPES).includes(operation)) {
    throw new KafkaJSNonRetriableError(
      `Invalid config operation ${inspect(operation)} for ${name}: use a value of AlterConfigOpTypes`
    )
  }

  if (operation !== ALTER_CONFIG_OP_TYPES.DELETE && typeof value !== 'string') {
    const operationName = Object.keys(ALTER_CONFIG_OP_TYPES).find(
      (key) => ALTER_CONFIG_OP_TYPES[key] === operation
    )
    throw new KafkaJSNonRetriableError(
      `Invalid config value for ${name}: ${operationName} needs a string, got ${inspect(value)}`
    )
  }
}

/**
 * Checks the config entries of an `incrementalAlterConfigs` request, so that a mistake costs no
 * round trip and names the config: the broker rejects a repeated resource or config name with less
 * context, and the 4.3 controller applies a SET without a value as the removal of the config.
 * @param {{ type: number, name: string, configEntries: unknown }[]} resources already past
 *   {@link assertResourceTypesAndNames}
 * @returns {void}
 * @throws {KafkaJSNonRetriableError} when a resource repeats (names written as the same bytes, as
 *   lone surrogates are, repeat it), `configEntries` is not an array, an entry fails
 *   {@link assertIncrementalEntry}, or a config name repeats in one resource whatever its operation
 */
const assertIncrementalEntries = (resources) => {
  const seenResources = new Set()

  for (const { type, name, configEntries } of resources) {
    const resourceKey = `${type}/${name}`
    const sentKey = `${type}/${Encoder.sentForm(name)}`

    if (seenResources.has(sentKey)) {
      throw new KafkaJSNonRetriableError(
        `Invalid resources array, it cannot have multiple entries for the same resource: ${JSON.stringify({ type, name })}`
      )
    }
    seenResources.add(sentKey)

    if (!Array.isArray(configEntries)) {
      throw new KafkaJSNonRetriableError(
        `Invalid resource configEntries ${inspect(configEntries)}: ${JSON.stringify({ type, name })}`
      )
    }

    const seenNames = new Set()

    for (const entry of configEntries) {
      assertIncrementalEntry(entry, resourceKey)

      if (seenNames.has(entry.name)) {
        throw new KafkaJSNonRetriableError(
          `Invalid resource configEntries, it cannot have multiple entries for the config ${entry.name} of resource ${resourceKey}`
        )
      }
      seenNames.add(entry.name)
    }
  }
}

/**
 * Puts per-broker `incrementalAlterConfigs` results back in the order the caller gave the
 * resources. A resource the brokers did not answer gets an `UNKNOWN` result, never a success. A
 * broker names a resource as the bytes it read (a lone surrogate is written as U+FFFD), so results
 * are matched by that name and returned under the name the caller gave.
 * @param {{ type: number, name: string }[]} resources the request, in the caller's order
 * @param {{ errorCode: number, errorMessage: string|null, resourceType: number,
 *   resourceName: string }[]} results every broker's results, in any order
 * @returns {{ errorCode: number, errorMessage: string|null, resourceType: number,
 *   resourceName: string }[]} one result per resource
 */
const inRequestOrder = (resources, results) => {
  const answered = new Map(
    results.map((result) => [`${result.resourceType}/${result.resourceName}`, result])
  )

  return resources.map(({ type, name }) => {
    const result = answered.get(`${type}/${Encoder.sentForm(name)}`)

    if (result === undefined) {
      return {
        errorCode: -1,
        errorMessage: 'The broker did not answer for this resource',
        resourceType: type,
        resourceName: name,
      }
    }

    return result.resourceName === name ? result : { ...result, resourceName: name }
  })
}

/**
 * One ListGroups row in its public form.
 *
 * `groupType` is lower-case on this API while the wire carries a display name, so the mapping is
 * done here rather than in the broker method, whose job is the wire. It goes through the shared
 * table rather than a bare `toLowerCase()`, so the public union and the protocol table cannot drift
 * apart; a value the table does not know is passed through lower-cased rather than dropped.
 * `groupState` deliberately keeps its wire casing: that is what this client has always returned and
 * what `ConsumerGroupState` types.
 * @param {{ groupId: string, protocolType: string, groupState?: string, groupType?: string }} row
 * @returns {{ groupId: string, protocolType: string, groupState: string, groupType: string }}
 */
const toPublicGroupOverview = (row) => {
  const known = Object.keys(GROUP_TYPES).find((name) => GROUP_TYPES[name] === row.groupType)

  return {
    groupId: row.groupId,
    protocolType: row.protocolType,
    groupState: row.groupState || '',
    groupType: known || (row.groupType ? row.groupType.toLowerCase() : ''),
  }
}

/**
 *
 * @param {Object} params
 * @param {import("../../types").Logger} params.logger
 * @param {InstrumentationEventEmitter} [params.instrumentationEmitter]
 * @param {import('../../types').RetryOptions} params.retry
 * @param {import("../../types").Cluster} params.cluster
 * @param {import('../telemetry/factory').TelemetryFactory|null} [params.telemetry=null] private
 *   telemetry binding: its three public methods are added to the admin, it resumes after a usable
 *   connect and joins the one close
 * @param {number} [params.apiTimeout=60000] validated default end-to-end call budget
 *
 * @returns {import("../../types").Admin}
 */
module.exports = ({
  logger: rootLogger,
  instrumentationEmitter: rootInstrumentationEmitter,
  retry,
  cluster,
  apiTimeout = 60000,
  telemetry = null,
}) => {
  apiTimeout = validateApiTimeout(apiTimeout)
  const logger = rootLogger.namespace('Admin')
  const instrumentationEmitter = rootInstrumentationEmitter || new InstrumentationEventEmitter()
  const shareGroups = createShareGroups({ cluster, retry })
  const offsetAdmin = new OffsetAdmin({ cluster, retry })

  const lifecycle = new AdminLifecycle({
    cluster,
    logger,
    onDisconnect: () => instrumentationEmitter.emit(DISCONNECT),
    telemetry,
  })

  /**
   * Reopens only after the preceding close finishes; old setup can never publish into it.
   * @returns {Promise<void>}
   * @throws {Error} original setup error or captured lifecycle refusal
   */
  const connect = () =>
    lifecycle.connect(async (generation) => {
      await generation.wait(sharedSetup(() => cluster.connect({ fence: generation })))
      const error = generation.guard()
      if (error) throw error
      telemetry?.resume()
      instrumentationEmitter.emit(CONNECT)
    })

  /**
   * Drains admitted calls, or closes their captured lifecycle by the first call's deadline.
   * @param {{ timeout?: number }|unknown} [options] integer milliseconds; legacy arguments ignored
   * @returns {Promise<void>} shared cleanup episode
   * @throws {import('../errors').KafkaJSInvalidConfigurationError} rejected before mutation
   * @throws {Error} original transport cleanup failure when draining naturally
   */
  const disconnect = (options) => lifecycle.disconnect(options)

  /**
   * @return {Promise}
   */
  const listTopics = async () => {
    const { topicMetadata } = checked(await waitOf(async () => cluster.metadata()))
    const topics = topicMetadata.map((t) => t.topic)
    return topics
  }

  /**
   * Creates topics. A failure that names only some topics does not repeat the others: each retry
   * sends just the topics the broker said could still go through (`retriable`), a topic it
   * confirmed is never sent again, and a topic it refused for good is remembered across the
   * retries.
   * @param {Object} request
   * @param {array} request.topics
   * @param {boolean} [request.validateOnly=false]
   * @param {number} [request.timeout=5000]
   * @param {boolean} [request.waitForLeaders=true] after the last attempt, waits for the leaders of
   *   the topics this call created (not for one that already existed or was refused)
   * @returns {Promise<boolean>} true when every topic was created; false when every topic that did
   *   not go through already existed (also when others were created)
   * @throws {KafkaJSAggregateError} when the attempts end with a topic refused for good: one error
   *   per such topic, not retriable, followed by the failures of an attempt that ran out of
   *   retries. `TOPIC_ALREADY_EXISTS` is benign: when it is the only refusal and nothing else
   *   failed, the call resolves `false` instead
   * @throws {KafkaJSNumberOfRetriesExceeded|Error} what ended the retries, unchanged, when no topic
   *   was refused for good
   * @throws {KafkaJSTimeout|Error} with `waitForLeaders`, a metadata error or a leader-wait timeout
   *   rejects the call directly, not as an aggregate; a call whose attempts fail never enters
   *   that wait
   */
  const createTopics = async ({ topics, validateOnly, timeout, waitForLeaders = true }) => {
    if (!topics || !Array.isArray(topics)) {
      throw new KafkaJSNonRetriableError(`Invalid topics array ${topics}`)
    }

    if (topics.filter(({ topic }) => typeof topic !== 'string').length > 0) {
      throw new KafkaJSNonRetriableError(
        'Invalid topics array, the topic names have to be a valid string'
      )
    }

    const topicNames = new Set(topics.map(({ topic }) => topic))
    if (topicNames.size < topics.length) {
      throw new KafkaJSNonRetriableError(
        'Invalid topics array, it cannot have multiple entries for the same topic'
      )
    }

    for (const { topic, configEntries } of topics) {
      if (configEntries == null) {
        continue
      }

      if (!Array.isArray(configEntries)) {
        throw new KafkaJSNonRetriableError(
          `Invalid configEntries for topic "${topic}", must be an array`
        )
      }

      configEntries.forEach((entry, index) => {
        if (typeof entry !== 'object' || entry == null) {
          throw new KafkaJSNonRetriableError(
            `Invalid configEntries for topic "${topic}". Entry ${index} must be an object`
          )
        }

        for (const requiredProperty of ['name', 'value']) {
          if (
            !Object.prototype.hasOwnProperty.call(entry, requiredProperty) ||
            typeof entry[requiredProperty] !== 'string'
          ) {
            throw new KafkaJSNonRetriableError(
              `Invalid configEntries for topic "${topic}". Entry ${index} must have a valid "${requiredProperty}" property`
            )
          }
        }
      })
    }

    const outcome = new AggregateOutcome({
      keys: topics.map(({ topic }) => topic),
      keyOf: ({ topic }) => topic,
      select: (names) => topics.filter(({ topic }) => names.has(topic)),
      message: 'Topic creation errors',
      isBenign: (child) => child?.type === 'TOPIC_ALREADY_EXISTS',
    })
    const retrier = retrierOf(retry)
    let broker
    let outstanding

    try {
      checked(
        await waitOf(async () =>
          retrier(async (bail, retryCount, retryTime) => {
            try {
              checked(await waitOf(async () => cluster.refreshMetadata()))
              broker = checked(await waitOf(async () => cluster.findControllerBroker()))
              checked(
                await waitOf(async () =>
                  broker.createTopics({ topics: outcome.pending, validateOnly, timeout })
                )
              )
              outcome.succeeded()
            } catch (e) {
              if (e.type === 'NOT_CONTROLLER') {
                logger.warn('Could not create topics', { error: e.message, retryCount, retryTime })
                throw e
              }

              if (isPerItemFailure(e)) {
                const retryable = outcome.settle(e)
                if (retryable) {
                  logger.warn('Could not create some topics yet', {
                    topics: retryable.errors.map(({ topic }) => topic),
                    retryCount,
                    retryTime,
                  })
                  throw retryable
                }
                return
              }

              bail(e)
            }
          })
        )
      )
    } catch (e) {
      outstanding = e
    }

    const everyTopicCreated = outcome.finish(outstanding)

    if (!validateOnly && waitForLeaders && outcome.created.size > 0) {
      checked(
        await waitOf(async () =>
          retryOnLeaderNotAvailable(
            async () => checked(await waitOf(async () => broker.metadata([...outcome.created]))),
            {
              delay: 100,
              maxWait: timeout,
              timeoutMessage: 'Timed out while waiting for topic leaders',
              // a validateOnly request creates nothing, so no topic is on its way to the broker
              waitForTopicPropagation: !validateOnly,
            }
          )
        )
      )
    }

    return everyTopicCreated
  }
  /**
   * @param {array} topicPartitions
   * @param {boolean} [validateOnly=false]
   * @param {number} [timeout=5000]
   * @return {Promise<void>}
   */
  const createPartitions = async ({ topicPartitions, validateOnly, timeout }) => {
    if (!topicPartitions || !Array.isArray(topicPartitions)) {
      throw new KafkaJSNonRetriableError(`Invalid topic partitions array ${topicPartitions}`)
    }
    if (topicPartitions.length === 0) {
      throw new KafkaJSNonRetriableError(`Empty topic partitions array`)
    }

    if (topicPartitions.filter(({ topic }) => typeof topic !== 'string').length > 0) {
      throw new KafkaJSNonRetriableError(
        'Invalid topic partitions array, the topic names have to be a valid string'
      )
    }

    const topicNames = new Set(topicPartitions.map(({ topic }) => topic))
    if (topicNames.size < topicPartitions.length) {
      throw new KafkaJSNonRetriableError(
        'Invalid topic partitions array, it cannot have multiple entries for the same topic'
      )
    }

    const retrier = retrierOf(retry)
    let pending = topicPartitions

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        checked(await waitOf(async () => cluster.refreshMetadata()))
        const broker = checked(await waitOf(async () => cluster.findControllerBroker()))
        checked(
          await waitOf(async () =>
            broker.createPartitions({ topicPartitions: pending, validateOnly, timeout })
          )
        )
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not create topics', { error: e.message, retryCount, retryTime })
          throw e
        }

        const refused = throttledTopics(e)

        if (refused) {
          pending = pending.filter(({ topic }) => refused.has(topic))
          logger.warn('A quota refused partitions', { topics: [...refused], retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * One DeleteTopics attempt by id, inside a `deleteTopics` retrier. A confirmed id leaves the
   * pending set and, while metadata still maps its name to it, the admin's target topics. When
   * every topic the controller answered with an error was refused by its mutation quota, the
   * pending set narrows to those ids and the retrier sends them again; any other answered error
   * ends the call.
   * @param {{pending: string[], knownNames: Map<string, string>, names: Map<string, string>}} state
   *   ids still to delete, their names as last known, and the caller's name for each id of a
   *   by-name call (its errors name topics as the caller did)
   * @param {number} [timeout] broker timeout
   * @param {Function} bail ends the retries with its error
   * @returns {Promise<void>}
   * @throws {Error} a failure the retrier may try again
   */
  const deleteTopicIdsOnce = async (state, timeout, bail) => {
    const forget = (confirmed) => {
      for (const id of confirmed) {
        const name = state.knownNames.get(id)
        if (name !== undefined && cluster.metadataCache?.topicId(name) === id)
          cluster.targetTopics.delete(name)
      }
    }
    try {
      checked(await waitOf(() => cluster.refreshMetadata()))
      for (const id of state.pending) {
        const name = cluster.metadataCache?.topicName(id)
        if (name !== undefined && !state.knownNames.has(id)) state.knownNames.set(id, name)
      }
      const broker = checked(await waitOf(() => cluster.findControllerBroker()))
      checked(await waitOf(() => broker.deleteTopics({ topicIds: state.pending, timeout })))
      const confirmed = state.pending
      state.pending = []
      forget(confirmed)
    } catch (error) {
      const failures = error.topicErrors?.map((row) => ({
        ...row,
        topic: state.names.get(row.topicId) ?? row.topic ?? row.topicId,
      }))
      if (failures) error.topicErrors = failures
      if (failures) {
        const failed = new Set(failures.map((row) => row.topicId))
        forget(state.pending.filter((id) => !failed.has(id)))
      }
      if (endsCall(error) || isAnsweredError(error)) return bail(error)
      if (
        failures?.length > 0 &&
        failures.every((row) => row.errorCode === THROTTLING_QUOTA_EXCEEDED_CODE)
      ) {
        const refused = new Set(failures.map((row) => row.topicId))
        state.pending = state.pending.filter((id) => refused.has(id))
        if (state.pending.length === 0) return bail(error)
        throw error
      }
      if (failures || !DELETE_TOPICS_RETRIED.has(error.type)) return bail(error)
      if (error.type === 'NOT_CONTROLLER') checked(await waitOf(() => cluster.refreshMetadata()))
      throw error
    }
  }

  /** Deletes names or canonical IDs with the existing void/throw contract.
   * ID responses must cover the attempted set before cleanup. Confirmed IDs leave the mutation
   * set before follow-up metadata refresh; metadata retries share the invocation deadline.
   * When every topic the controller answered with an error was refused by its mutation quota
   * (`THROTTLING_QUOTA_EXCEEDED`), only those topics are sent again within the `retry` budget, by
   * topic id: a by-name call sends a refused topic again by the id the answer gave for it, so a
   * topic recreated under that name meanwhile is not deleted, and a refusal that gives no usable id
   * (DeleteTopics v5 and older, or a null id) is final. Errors still name the topics the caller
   * passed. Any other answered error is final and a written request with no answer rejects with
   * `KafkaJSUnknownOutcomeError`.
   * @param {object} options exclusive topics or topicIds, with optional broker timeout
   * @returns {Promise<void>} deletion and follow-up metadata refresh completed
   * @throws {Error} validation, protocol, discovery or invocation failure
   */
  const deleteTopics = async ({ topics, topicIds, timeout } = {}) => {
    if ((topics !== undefined) === (topicIds !== undefined))
      throw new KafkaJSInvalidConfigurationError('Exactly one of topics or topicIds is required')
    if (topicIds !== undefined) {
      if (
        !Array.isArray(topicIds) ||
        topicIds.length === 0 ||
        topicIds.some(
          (id) =>
            typeof id !== 'string' ||
            !/^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$/.test(id) ||
            /^0{8}(-0{4}){3}-0{12}$/.test(id)
        )
      )
        throw new KafkaJSInvalidConfigurationError('Invalid topicIds')
      const ids = topicIds.map((id) => id.toLowerCase())
      if (new Set(ids).size !== ids.length)
        throw new KafkaJSInvalidConfigurationError('Duplicate topicIds')
      const byId = { pending: ids, knownNames: new Map(), names: new Map() }
      await retrierOf(retry)((bail) => deleteTopicIdsOnce(byId, timeout, bail))
      return retrierOf(retry)(async (bail) => {
        try {
          checked(await waitOf(() => cluster.refreshMetadata()))
        } catch (error) {
          if (error.type === 'NOT_CONTROLLER') throw error
          bail(error)
        }
      })
    }
    if (!topics || !Array.isArray(topics)) {
      throw new KafkaJSNonRetriableError(`Invalid topics array ${topics}`)
    }

    if (topics.filter((topic) => typeof topic !== 'string').length > 0) {
      throw new KafkaJSNonRetriableError('Invalid topics array, the names must be a valid string')
    }

    const names = topics.slice()
    let byId = null
    await retrierOf(retry)(async (bail, retryCount, retryTime) => {
      if (byId) return deleteTopicIdsOnce(byId, timeout, bail)
      try {
        checked(await waitOf(() => cluster.refreshMetadata()))
        const broker = checked(await waitOf(() => cluster.findControllerBroker()))
        checked(await waitOf(() => broker.deleteTopics({ topics: names, timeout })))
      } catch (error) {
        if (error.topicErrors) {
          const failed = new Set(error.topicErrors.map(({ topic }) => topic))
          for (const topic of names) {
            if (!failed.has(topic)) cluster.targetTopics.delete(topic)
          }
        }
        const refused = !endsCall(error) && !isAnsweredError(error) && quotaRefusedIds(error, names)
        if (refused) {
          byId = { pending: [...refused.keys()], knownNames: new Map(refused), names: refused }
          logger.warn('A quota refused topic deletions', {
            topics: [...refused.values()],
            retryCount,
            retryTime,
          })
          throw error
        }
        if (error.type === 'REQUEST_TIMED_OUT') {
          logger.error(
            'Could not delete topics, check if "delete.topic.enable" is set to "true" (the default value is "false") or increase the timeout',
            { error: error.message, retryCount, retryTime }
          )
        }
        if (endsCall(error) || isAnsweredError(error) || error.topicErrors) return bail(error)
        if (DELETE_TOPICS_RETRIED.has(error.type)) {
          logger.warn('Could not delete topics', { error: error.message, retryCount, retryTime })
          throw error
        }
        return bail(error)
      }
      for (const topic of names) cluster.targetTopics.delete(topic)
    })
    return retrierOf(retry)(async (bail) => {
      try {
        checked(await waitOf(() => cluster.refreshMetadata()))
      } catch (error) {
        if (error.type === 'NOT_CONTROLLER') throw error
        bail(error)
      }
    })
  }

  /**
   * Reads low and high offsets at the selected isolation; the default remains committed.
   * @param {string} topic topic name
   * @param {object} [options] isolationLevel and apiTimeout
   * @returns {Promise<object[]>} partition low/high decimal offsets
   * @throws {Error} validation, deadline or broker failure
   */

  const fetchTopicOffsets = async (topic, options) => {
    const isolationLevel = isolationOf(options)
    if (!topic || typeof topic !== 'string') {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }

    const retrier = retrierOf(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        checked(await waitOf(async () => cluster.addTargetTopic(topic)))
        checked(await waitOf(async () => cluster.refreshMetadataIfNecessary()))

        const metadata = cluster.findTopicPartitionMetadata(topic)
        const high = checked(
          await waitOf(async () =>
            cluster.fetchTopicsOffset(
              [
                {
                  topic,
                  fromBeginning: false,
                  partitions: metadata.map((p) => ({ partition: p.partitionId })),
                },
              ],
              { isolationLevel }
            )
          )
        )

        const low = checked(
          await waitOf(async () =>
            cluster.fetchTopicsOffset(
              [
                {
                  topic,
                  fromBeginning: true,
                  partitions: metadata.map((p) => ({ partition: p.partitionId })),
                },
              ],
              { isolationLevel }
            )
          )
        )

        const { partitions: highPartitions } = high.pop()
        const { partitions: lowPartitions } = low.pop()
        return highPartitions.map(({ partition, offset }) => ({
          partition,
          offset,
          high: offset,
          low: lowPartitions.find(({ partition: lowPartition }) => lowPartition === partition)
            .offset,
        }))
      } catch (e) {
        // Stale metadata - an unknown or moved partition, or a leader epoch the broker fenced -
        // is refreshed and retried; anything else fails fast.
        if (staleMetadata(e) || staleLeaderEpoch(e)) {
          checked(await waitOf(async () => cluster.refreshMetadata()))
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * @param {string} topic
   * @param {number|string} [timestamp] milliseconds, a decimal timestamp string or a named spec
   * @param {object} [options] isolationLevel and apiTimeout
   * @returns {Promise<object[]>} partition decimal offsets, falling back to high on no match
   * @throws {Error} validation, deadline or broker failure
   */

  const fetchTopicOffsetsByTimestamp = async (topic, timestamp, options) => {
    const isolationLevel = isolationOf(options)
    const specs = {
      maxTimestamp: [-3, 7],
      earliestLocal: [-4, 8],
      latestTiered: [-5, 9],
      earliestPendingUpload: [-6, 11],
    }
    if (typeof timestamp === 'string' && !/^-?\d+$/.test(timestamp)) {
      if (!Object.prototype.hasOwnProperty.call(specs, timestamp))
        throw new KafkaJSInvalidConfigurationError('Invalid offset spec')
      if (!topic || typeof topic !== 'string')
        throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
      const [fromTimestamp, minVersion] = specs[timestamp]
      return retrierOf(retry)(async (bail) => {
        try {
          checked(await waitOf(() => cluster.addTargetTopic(topic)))
          checked(await waitOf(() => cluster.refreshMetadataIfNecessary()))
          const partitions = cluster
            .findTopicPartitionMetadata(topic)
            .map((p) => ({ partition: p.partitionId }))
          const rows = checked(
            await waitOf(() =>
              cluster.fetchTopicsOffset([{ topic, fromTimestamp, partitions }], {
                isolationLevel,
                minVersion,
                maxVersion: 11,
                timeout: 5000,
              })
            )
          )
          return rows[0].partitions.map(({ partition, offset }) => ({ partition, offset }))
        } catch (error) {
          if (staleMetadata(error) || staleLeaderEpoch(error)) {
            checked(await waitOf(() => cluster.refreshMetadata()))
            throw error
          }
          bail(error)
        }
      })
    }
    if (!topic || typeof topic !== 'string') {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }

    const retrier = retrierOf(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        checked(await waitOf(async () => cluster.addTargetTopic(topic)))
        checked(await waitOf(async () => cluster.refreshMetadataIfNecessary()))

        const metadata = cluster.findTopicPartitionMetadata(topic)
        const partitions = metadata.map((p) => ({ partition: p.partitionId }))

        const high = checked(
          await waitOf(async () =>
            cluster.fetchTopicsOffset(
              [
                {
                  topic,
                  fromBeginning: false,
                  partitions,
                },
              ],
              { isolationLevel }
            )
          )
        )
        const { partitions: highPartitions } = high.pop()

        const offsets = checked(
          await waitOf(async () =>
            cluster.fetchTopicsOffset(
              [
                {
                  topic,
                  fromTimestamp: timestamp,
                  partitions,
                },
              ],
              { isolationLevel }
            )
          )
        )
        const { partitions: lowPartitions } = offsets.pop()

        return lowPartitions.map(({ partition, offset }) => ({
          partition,
          offset:
            parseInt(offset, 10) >= 0
              ? offset
              : highPartitions.find(({ partition: highPartition }) => highPartition === partition)
                  .offset,
        }))
      } catch (e) {
        // Stale metadata - an unknown or moved partition, or a leader epoch the broker fenced -
        // is refreshed and retried; anything else fails fast.
        if (staleMetadata(e) || staleLeaderEpoch(e)) {
          checked(await waitOf(async () => cluster.refreshMetadata()))
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * Fetch offsets for a topic or multiple topics
   *
   * Note: set either topic or topics but not both.
   *
   * @param {string} groupId
   * @param {string[]} topics - list of topics to fetch offsets for, defaults to `[]` which fetches all topics for `groupId`.
   * @param {boolean} [requireStable=false] request stable commits; requires OffsetFetch v7+
   * @param {boolean} [resolveOffsets=false]
   * @return {Promise}
   */
  const fetchOffsets = async ({
    groupId,
    topics,
    resolveOffsets = false,
    requireStable = false,
  }) => {
    if (typeof requireStable !== 'boolean')
      throw new KafkaJSInvalidConfigurationError('Invalid requireStable')
    if (!groupId) {
      throw new KafkaJSNonRetriableError(`Invalid groupId ${groupId}`)
    }

    if (!topics) {
      topics = []
    }

    if (!Array.isArray(topics)) {
      throw new KafkaJSNonRetriableError('Expected topics array to be set')
    }

    const coordinator = checked(await waitOf(async () => cluster.findGroupCoordinator({ groupId })))
    const topicsToFetch = checked(
      await waitOf(async () =>
        Promise.all(
          topics.map(async (topic) => {
            const partitions = checked(
              await waitOf(async () => findTopicPartitions(cluster, topic))
            )
            const partitionsToFetch = partitions.map((partition) => ({ partition }))
            return { topic, partitions: partitionsToFetch }
          })
        )
      )
    )
    let { responses: consumerOffsets } = checked(
      await waitOf(async () =>
        coordinator.offsetFetch({
          groupId,
          topics: topicsToFetch,
          ...(requireStable ? { requireStable } : {}),
        })
      )
    )

    if (resolveOffsets) {
      consumerOffsets = checked(
        await waitOf(async () =>
          Promise.all(
            consumerOffsets.map(async ({ topic, partitions }) => {
              const indexedOffsets = indexByPartition(
                checked(await waitOf(async () => fetchTopicOffsets(topic)))
              )
              const recalculatedPartitions = partitions.map(({ offset, partition, ...props }) => {
                let resolvedOffset = offset
                if (Number(offset) === EARLIEST_OFFSET) {
                  resolvedOffset = indexedOffsets[partition].low
                }
                if (Number(offset) === LATEST_OFFSET) {
                  resolvedOffset = indexedOffsets[partition].high
                }
                return {
                  partition,
                  offset: resolvedOffset,
                  ...props,
                }
              })

              checked(
                await waitOf(async () =>
                  setOffsets({ groupId, topic, partitions: recalculatedPartitions })
                )
              )

              return {
                topic,
                partitions: recalculatedPartitions,
              }
            })
          )
        )
      )
    }

    return consumerOffsets.map(({ topic, partitions }) => {
      const completePartitions = partitions.map(({ partition, offset, metadata }) => ({
        partition,
        offset,
        metadata: metadata || null,
      }))

      return { topic, partitions: completePartitions }
    })
  }

  /**
   * @param {string} groupId
   * @param {string} topic
   * @param {boolean} [earliest=false]
   * @return {Promise}
   */
  const resetOffsets = async ({ groupId, topic, earliest = false }) => {
    if (!groupId) {
      throw new KafkaJSNonRetriableError(`Invalid groupId ${groupId}`)
    }

    if (!topic) {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }

    const partitions = checked(await waitOf(async () => findTopicPartitions(cluster, topic)))
    const partitionsToSeek = partitions.map((partition) => ({
      partition,
      offset: cluster.defaultOffset({ fromBeginning: earliest }),
    }))

    return setOffsets({ groupId, topic, partitions: partitionsToSeek })
  }

  /**
   * @typedef {Object} SeekEntry
   * @property {number} partition
   * @property {string} offset
   */

  /**
   * Sets the committed offsets of a consumer group that has no running members. The admin writes
   * them itself, with one OffsetCommit to the group's coordinator that carries no member id and
   * generation -1, as the Java admin's `alterConsumerGroupOffsets` does; it never joins the group.
   * The last entry for a partition wins, and every named partition is sent, in partition order: the
   * coordinator refuses one the topic does not have with `UNKNOWN_TOPIC_OR_PARTITION`. Each commit
   * carries only the partition and offset, so stored metadata becomes empty and the leader epoch
   * unknown. An offset of `-1` (latest) or `-2` (earliest) is committed as
   * given and stays that marker until it is resolved (`fetchOffsets` with `resolveOffsets` does so,
   * and a consumer without a seek applies its reset policy to it). A group that does not exist is
   * created by the coordinator. A member that starts during the call makes the coordinator refuse
   * the commit with `UNKNOWN_MEMBER_ID`. Partitions answered with a retriable coordinator code are
   * sent again within the retry budget; a confirmed partition is never sent again by this call.
   * A `KafkaJSAggregateError` names every partition not confirmed, and the partitions it does not
   * name were committed; any other rejection means no partition was confirmed. A commit that timed
   * out may still have been applied. On a classic group that is already empty the commit does not
   * restart the group's empty-since time, so these offsets can expire `offsets.retention.minutes`
   * after the group became empty rather than after this call. Sending the call again writes the offsets again,
   * over anything committed in between.
   * @param {object} request
   * @param {string} request.groupId
   * @param {string} request.topic
   * @param {Array<SeekEntry>} request.partitions at least one entry
   * @returns {Promise<void>} once the coordinator accepted every offset
   * @throws {KafkaJSNonRetriableError} before any request, for a missing `groupId` or `topic`, no
   *   `partitions`, an entry whose partition is not a partition id, or an offset that is not a
   *   decimal integer string or safe integer number of at least 0 (and at most the int64 maximum)
   *   or exactly `-1`/`-2`; and, before any commit, for a group whose state is
   *   not `Empty` or `Dead`
   * @throws {KafkaJSProtocolError} `UNKNOWN_TOPIC_OR_PARTITION` for a topic the cluster does not
   *   have, or the code a group description was refused with
   * @throws {KafkaJSAggregateError} one child per partition not committed (see
   *   `OffsetAdmin#commitOffsets`)
   * @throws {Error} the commit request's own failure, a timeout say, when no partition was confirmed
   *   or answered with a code
   * @throws {KafkaJSInvariantViolation} for a malformed commit reply
   * @throws {KafkaJSTimeout} once the API deadline passed
   */
  const setOffsets = async ({ groupId, topic, partitions }) => {
    if (!groupId) {
      throw new KafkaJSNonRetriableError(`Invalid groupId ${groupId}`)
    }

    if (!topic) {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }

    if (!partitions || partitions.length === 0) {
      throw new KafkaJSNonRetriableError(`Invalid partitions`)
    }
    for (const { partition } of partitions) assertPartitionId(partition)
    const offsets = new Map(
      partitions.map(({ partition, offset }) => [partition, commitOffsetOf(offset)])
    )

    const existing = checked(await waitOf(async () => findTopicPartitions(cluster, topic)))
    if (existing.length === 0) throw createErrorFromCode(UNKNOWN_TOPIC_OR_PARTITION_CODE)

    const { groups } = checked(await waitOf(async () => describeGroups([groupId])))
    const group = groups.find((row) => row.groupId === groupId)
    if (group === undefined) {
      throw new KafkaJSNonRetriableError(`The coordinator described no group for ${groupId}`)
    }
    const missing = group.state === 'Dead' && group.errorCode === GROUP_ID_NOT_FOUND_CODE
    if (group.errorCode !== 0 && !missing) throw createErrorFromCode(group.errorCode)
    if (!isConsumerGroupRunning(group)) {
      throw new KafkaJSNonRetriableError(
        `The consumer group must have no running instances, current state: ${group.state}`
      )
    }

    const cells = [...offsets]
      .sort(([a], [b]) => a - b)
      .map(([partition, offset]) => ({ partition, offset }))
    await offsetAdmin.commitOffsets({ groupId, topic, cells })
  }

  /**
   * A BROKER_LOGGER resource and a BROKER resource with a name belong to the one broker named by
   * the resource; a BROKER resource with the empty name is the cluster-wide default, which goes to
   * the controller like every other resource.
   * @param {{ type: number, name: string }} resource
   * @returns {boolean}
   */
  const isBrokerConfig = ({ type, name }) =>
    type === CONFIG_RESOURCE_TYPES.BROKER_LOGGER ||
    (type === CONFIG_RESOURCE_TYPES.BROKER && name !== '')

  /**
   * Broker configs can only be returned by the target broker; every other resource, the
   * cluster-default broker config included, goes to `defaultBroker`
   *
   * @see
   * https://github.com/apache/kafka/blob/821c1ac6641845aeca96a43bc2b946ecec5cba4f/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java#L3783
   * https://github.com/apache/kafka/blob/821c1ac6641845aeca96a43bc2b946ecec5cba4f/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java#L2027
   *
   * @param {Broker} defaultBroker. Broker used in case the configuration is not a broker config
   */
  const groupResourcesByBroker = ({ resources, defaultBroker }) =>
    groupBy(resources, async (resource) => {
      return isBrokerConfig(resource)
        ? checked(await waitOf(async () => cluster.findBroker({ nodeId: String(resource.name) })))
        : defaultBroker
    })

  /**
   * A BROKER resource with the empty name is the cluster-wide default broker config; every other
   * type needs a non-empty name.
   *
   * @param {Array<ResourceConfigQuery>} resources
   * @param {boolean} [includeSynonyms=false]
   * @param {boolean} [includeDocumentation=false] ask for each entry's documentation, which the
   *   broker leaves out unless asked; sent from DescribeConfigs v3
   * @return {Promise}
   *
   * @typedef {Object} ResourceConfigQuery
   * @property {ConfigResourceType} type
   * @property {string} name
   * @property {Array<String>} [configNames=[]]
   */
  const describeConfigs = async ({ resources, includeSynonyms, includeDocumentation }) => {
    assertResourcesArray(resources)
    assertResourceTypesAndNames(resources)

    const invalidConfigs = resources.find(
      (r) => !Array.isArray(r.configNames) && r.configNames != null
    )

    if (invalidConfigs) {
      const { configNames } = invalidConfigs
      throw new KafkaJSNonRetriableError(
        `Invalid resource configNames ${configNames}: ${JSON.stringify(invalidConfigs)}`
      )
    }

    const retrier = retrierOf(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        checked(await waitOf(async () => cluster.refreshMetadata()))
        const controller = checked(await waitOf(async () => cluster.findControllerBroker()))
        const resourcerByBroker = checked(
          await waitOf(async () =>
            groupResourcesByBroker({
              resources,
              defaultBroker: controller,
            })
          )
        )

        const describeConfigsAction = async (broker) => {
          const targetBroker = broker || controller
          return targetBroker.describeConfigs({
            resources: resourcerByBroker.get(targetBroker),
            includeSynonyms,
            includeDocumentation,
          })
        }

        const brokers = Array.from(resourcerByBroker.keys())
        const responses = checked(
          await waitOf(async () => Promise.all(brokers.map(describeConfigsAction)))
        )
        const responseResources = responses.reduce(
          (result, { resources }) => [...result, ...resources],
          []
        )

        return { resources: responseResources }
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not describe configs', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * Replaces the whole dynamic config of each resource: an entry left out is deleted.
   * {@link incrementalAlterConfigs} changes only the entries it names and is preferred. A BROKER
   * resource with the empty name is the cluster-wide default broker config; every other type needs
   * a non-empty name.
   *
   * @param {Array<ResourceConfig>} resources
   * @param {boolean} [validateOnly=false]
   * @return {Promise}
   *
   * @typedef {Object} ResourceConfig
   * @property {ConfigResourceType} type
   * @property {string} name
   * @property {Array<ResourceConfigEntry>} configEntries
   *
   * @typedef {Object} ResourceConfigEntry
   * @property {string} name
   * @property {string} value
   */
  const alterConfigs = async ({ resources, validateOnly }) => {
    assertResourcesArray(resources)
    assertResourceTypesAndNames(resources)

    const invalidConfigs = resources.find((r) => !Array.isArray(r.configEntries))

    if (invalidConfigs) {
      const { configEntries } = invalidConfigs
      throw new KafkaJSNonRetriableError(
        `Invalid resource configEntries ${configEntries}: ${JSON.stringify(invalidConfigs)}`
      )
    }

    const invalidConfigValue = resources.find((r) =>
      r.configEntries.some((e) => typeof e.name !== 'string' || typeof e.value !== 'string')
    )

    if (invalidConfigValue) {
      throw new KafkaJSNonRetriableError(
        `Invalid resource config value: ${JSON.stringify(invalidConfigValue)}`
      )
    }

    const retrier = retrierOf(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        checked(await waitOf(async () => cluster.refreshMetadata()))
        const controller = checked(await waitOf(async () => cluster.findControllerBroker()))
        const resourcerByBroker = checked(
          await waitOf(async () =>
            groupResourcesByBroker({
              resources,
              defaultBroker: controller,
            })
          )
        )

        const alterConfigsAction = async (broker) => {
          const targetBroker = broker || controller
          return targetBroker.alterConfigs({
            resources: resourcerByBroker.get(targetBroker),
            validateOnly: !!validateOnly,
          })
        }

        const brokers = Array.from(resourcerByBroker.keys())
        const responses = checked(
          await waitOf(() => {
            const sends = brokers.map(alterConfigsAction)
            return validateOnly ? Promise.all(sends) : settleMutations(sends)
          })
        )
        const responseResources = responses.reduce(
          (result, { resources }) => [...result, ...resources],
          []
        )

        return { resources: responseResources }
      } catch (e) {
        if (endsCall(e) || isAnsweredError(e)) return bail(e)
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not alter configs', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * Changes individual config entries (KIP-339): each entry names an operation, so the entries a
   * request leaves out stay as they are, unlike {@link alterConfigs}, which replaces the whole
   * config.
   *
   * A resource that fails does not fail the call: every resource gets a result in the order it was
   * given, and the caller reads its `errorCode`. Changes are atomic per resource and not across
   * resources. APPEND and SUBTRACT take a comma-separated list and apply to list configs only (any
   * other answers `INVALID_CONFIG`); SUBTRACT of every value leaves an empty list, not the default,
   * and DELETE restores the default. The `value` of a DELETE is ignored.
   *
   * Resources are routed as {@link groupResourcesByBroker} does, one request per broker. Only a
   * result of `NOT_CONTROLLER` is retried, by sending the whole call again, which is safe because
   * every operation can be repeated; any other code is a result and is never retried. A transport
   * failure rejects as it is.
   *
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-339%3A+Create+a+new+IncrementalAlterConfigs+API
   * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java
   *
   * @param {Object} options
   * @param {Array<IncrementalResourceConfig>} options.resources
   * @param {boolean} [options.validateOnly=false] validate the change without applying it
   * @returns {Promise<{ resources: Array<IncrementalAlterResult> }>} one result per resource
   * @throws {KafkaJSNonRetriableError} before anything is sent, for an invalid argument
   * @throws {KafkaJSNumberOfRetriesExceeded} when `NOT_CONTROLLER` persists
   *
   * @typedef {Object} IncrementalResourceConfig
   * @property {ConfigResourceType} type
   * @property {string} name the empty string names the cluster-default BROKER config
   * @property {Array<{ name: string, operation: AlterConfigOpTypes, value?: string|null }>} configEntries
   *
   * @typedef {Object} IncrementalAlterResult
   * @property {number} errorCode 0 on success
   * @property {string|null} errorMessage
   * @property {ConfigResourceType} resourceType
   * @property {string} resourceName
   */
  const incrementalAlterConfigs = async ({ resources, validateOnly = false }) => {
    assertResourcesArray(resources)

    const emptyAt = resources.findIndex((resource) => resource == null)

    if (emptyAt !== -1) {
      throw new KafkaJSNonRetriableError(
        `Invalid resources array, the entry at index ${emptyAt} is ${resources[emptyAt]}`
      )
    }

    assertResourceTypesAndNames(resources)
    assertIncrementalEntries(resources)

    const retrier = retrierOf(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        checked(await waitOf(async () => cluster.refreshMetadata()))
        const controller = checked(await waitOf(async () => cluster.findControllerBroker()))
        const resourcesByBroker = checked(
          await waitOf(async () =>
            groupResourcesByBroker({
              resources,
              defaultBroker: controller,
            })
          )
        )
        const responses = checked(
          await waitOf(() => {
            const sends = Array.from(resourcesByBroker, async ([broker, group]) =>
              broker.incrementalAlterConfigs({ resources: group, validateOnly: !!validateOnly })
            )
            return validateOnly ? Promise.all(sends) : settleMutations(sends)
          })
        )
        const results = responses.flatMap((response) => response.resources)

        if (validateOnly && results.some(({ errorCode }) => errorCode === NOT_CONTROLLER_CODE)) {
          throw createErrorFromCode(NOT_CONTROLLER_CODE)
        }

        return { resources: inRequestOrder(resources, results) }
      } catch (e) {
        if (endsCall(e) || isAnsweredError(e)) return bail(e)
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not alter configs', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * Reads `names` through DescribeTopicPartitions inside the admin retrier, from any connected
   * broker (`findConnectedBroker`): every broker answers it, so a dead controller does not fail the
   * call. The first attempt refreshes the metadata only if none is loaded or it has expired. A
   * failure of the request itself that is marked retriable (a connection that dropped, a request
   * that timed out) refreshes the metadata and asks again, on whichever connected broker the
   * cluster then returns, as `cluster.metadata` does with each broker in turn; a topic's own error
   * is never retried: it rejects once every page is in. A topic recreated between pages is read
   * again from the start the same way.
   *
   * The whole read, including metadata refresh, every page and every attempt, inherits the
   * public call's `apiTimeout` (60000 ms by default). At its actual monotonic deadline it rejects with
   * `KafkaJSTimeout`, also with request timeout enforcement disabled. Expiry detaches local
   * waits, pages, retry timers and unsent requests. Shared discovery stops only when no live
   * caller remains; physical setup keeps its own budget. Already-written mutations may apply.
   * @param {string[]} names
   * @returns {Promise<TopicsMetadata>}
   * @throws {KafkaJSServerDoesNotSupportApiKey} when the broker does not offer the API
   * @throws {KafkaJSNumberOfRetriesExceeded} when a retriable failure outlives the retry budget
   * @throws {KafkaJSTimeout} when the read is not done at its deadline
   */
  const describeByPartitions = async (names, includeTopicAuthorizedOperations = false) => {
    const retrier = retrierOf(retry)
    const rows = checked(
      await waitOf(async () =>
        withReadDeadline((deadline) =>
          retrier(async (bail, retryCount, retryTime) => {
            try {
              checkDeadline(deadline)

              if (retryCount > 0) {
                checked(await waitOf(async () => cluster.refreshMetadata()))
              } else {
                checked(await waitOf(async () => cluster.refreshMetadataIfNecessary()))
              }

              checkDeadline(deadline)
              return checked(
                await waitOf(async () =>
                  readTopicPages(
                    checked(await waitOf(async () => cluster.findConnectedBroker())),
                    names,
                    deadline
                  )
                )
              )
            } catch (e) {
              if (e.retriable === true) {
                logger.warn('Could not describe topics', {
                  error: e.message,
                  retryCount,
                  retryTime,
                })
                throw e
              }

              bail(e)
            }
          })
        )
      )
    )

    return toTopicsMetadata(rows, { includeTopicAuthorizedOperations })
  }

  /**
   * Fetch metadata for provided topics.
   *
   * If no topics are provided fetch metadata for all topics.
   *
   * When the cluster allows auto topic creation, a not-yet-created topic answers
   * `UNKNOWN_TOPIC_OR_PARTITION` while KRaft creates it asynchronously through the controller;
   * this retries through that window instead of failing on the first attempt.
   *
   * @see https://kafka.apache.org/protocol#The_Messages_Metadata
   *
   * With `useDescribeTopicPartitions` and at least one topic, the topics are read through
   * DescribeTopicPartitions (KIP-966) instead: any broker answers, pages are followed to the end
   * (a broker whose pages repeat, or that leaves a topic unanswered, fails the call), each
   * partition also reports `eligibleLeaderReplicas` and `lastKnownElr`, a partition without a
   * leader is returned with `leader: -1` instead of being waited for (Kafka 4.1.2 and 4.3 brokers
   * leave its `partitionErrorCode` at 0; a non-zero code is passed through), and a topic that does
   * not exist fails the call rather than being created. A failed request is retried on the `retry`
   * budget, as the other admin calls are. Metadata discovery, every page and retry, and an
   * unsupported-API Metadata fallback share the public call's `apiTimeout` (60000 ms by default).
   * At that deadline the call rejects with `KafkaJSTimeout`; the fallback's shorter Metadata
   * polling cap is nested inside it, never added. The option applies only to topics named
   * explicitly: empty or omitted `topics` go through Metadata.
   *
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas
   *
   * @param {Object} [options]
   * @param {string[]} [options.topics]
   * @param {boolean} [options.useDescribeTopicPartitions=false]
   * @param {number} [options.apiTimeout] per-call budget in ms; defaults to the admin's configured budget
   * @return {Promise<TopicsMetadata>}
   * @throws {KafkaJSNonRetriableError} if a topic name is not a valid string
   *
   * @typedef {Object} TopicsMetadata
   * @property {Array<TopicMetadata>} topics
   *
   * @typedef {Object} TopicMetadata
   * @property {String} name
   * @property {Array<PartitionMetadata>} partitions
   * @property {String|null} topicId The topic's uuid; null when the broker sends none
   * @property {boolean} isInternal
   *
   * @typedef {Object} PartitionMetadata
   * @property {number} partitionErrorCode Response error code
   * @property {number} partitionId Topic partition id
   * @property {number} leader  The id of the broker acting as leader for this partition.
   * @property {Array<number>} replicas The set of all nodes that host this partition.
   * @property {Array<number>} isr The set of nodes that are in sync with the leader for this partition.
   * @property {number} [leaderEpoch]
   * @property {Array<number>|null} [eligibleLeaderReplicas] only with `useDescribeTopicPartitions`
   * @property {Array<number>|null} [lastKnownElr] only with `useDescribeTopicPartitions`
   */
  const fetchTopicMetadata = async ({
    topics = [],
    useDescribeTopicPartitions = false,
    includeTopicAuthorizedOperations = false,
  } = {}) => {
    if (typeof includeTopicAuthorizedOperations !== 'boolean') {
      throw new (require('../errors').KafkaJSInvalidConfigurationError)(
        'includeTopicAuthorizedOperations must be a boolean'
      )
    }
    if (topics) {
      topics.forEach((topic) => {
        if (!topic || typeof topic !== 'string') {
          throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
        }
      })
    }

    if (useDescribeTopicPartitions && topics?.length > 0) {
      try {
        return checked(
          await waitOf(async () => describeByPartitions(topics, includeTopicAuthorizedOperations))
        )
      } catch (e) {
        if (e.name !== 'KafkaJSServerDoesNotSupportApiKey') {
          throw e
        }
      }
    }

    const metadata = checked(
      await waitOf(async () =>
        retryOnLeaderNotAvailable(
          () => cluster.metadata({ topics, includeTopicAuthorizedOperations }),
          {
            allowAutoTopicCreation: cluster.allowAutoTopicCreation,
          }
        )
      )
    )

    return {
      topics: metadata.topicMetadata.map((topicMetadata) => ({
        name: topicMetadata.topic,
        partitions: topicMetadata.partitionMetadata,
        topicId: topicMetadata.topicId,
        isInternal: topicMetadata.isInternal,
        authorizedOperations:
          includeTopicAuthorizedOperations &&
          topicMetadata.topicAuthorizedOperations != null &&
          topicMetadata.topicAuthorizedOperations !== -2147483648
            ? topicMetadata.topicAuthorizedOperations
            : null,
      })),
    }
  }

  /**
   * Lists the groups every broker knows about, optionally filtered by state and by type.
   *
   * The fan-out is sequential and all-or-nothing: one unreachable broker fails the whole call, as
   * it always has. A broker whose coordinator is still loading or not yet available answers with a
   * retriable error and is asked again on the configured retry budget; anything else fails at once.
   * Filters are lower-case here and are mapped to the wire display names by the broker method,
   * which also applies the version rules - asking for states needs ListGroups v4 and asking for
   * types needs v5, with one narrow exception for filters that a pre-v5 broker can answer correctly
   * by ignoring them. A filter the broker's version cannot serve is not retriable and fails on the
   * first attempt.
   *
   * @param {object} [options]
   * @param {string[]} [options.states=[]] e.g. `['stable', 'empty']`
   * @param {string[]} [options.types=[]] e.g. `['consumer', 'classic']`
   * Every per-broker call runs inside the retrier, so what a caller catches is the retrier's
   * rejection (`KafkaJSNonRetriableError`, or `KafkaJSNumberOfRetriesExceeded` once the budget is
   * spent) with the broker's error on `cause`, rather than the raw `KafkaJSProtocolError` 2.2.4
   * surfaced. `.type` and `.code` are carried through. The one exception is a filter this broker's
   * version cannot serve: that bails out of the retrier with the original error instance, so its
   * `apiKey` and `apiName` survive.
   *
   * @returns {Promise<{ groups: Array<{ groupId: string, protocolType: string, groupState: string, groupType: 'classic'|'consumer'|'share'|'streams'|'unknown'|'' }>}>}
   *          `groupType` is lower-case; `groupState` keeps the wire casing it has always had
   *          (`'Stable'`). Both are `''` against a broker whose version lacks them
   * @throws {KafkaJSNonRetriableError} when a filter is not an array of strings
   * @throws {KafkaJSServerDoesNotSupportApiKey} when a filter needs a version the broker does not
   *          offer
   */
  const listGroups = async ({ states = [], types = [] } = {}) => {
    assertStringArray('states', states)
    assertStringArray('types', types)

    checked(await waitOf(async () => cluster.refreshMetadata()))
    let groups = []
    for (const nodeId in cluster.brokerPool.brokers) {
      const broker = checked(await waitOf(async () => cluster.findBroker({ nodeId })))
      const response = checked(
        await waitOf(async () =>
          retrierOf(retry)(async (bail) => {
            try {
              return checked(await waitOf(async () => broker.listGroups({ states, types })))
            } catch (e) {
              if (e.name === 'KafkaJSServerDoesNotSupportApiKey') {
                // `bail` rejects with this instance rather than a wrapper, so the `apiKey`/`apiName`
                // that name the unsupported request reach the caller.
                return bail(e)
              }

              throw e
            }
          })
        )
      )
      groups = groups.concat(response.groups.map(toPublicGroupOverview))
    }

    return { groups }
  }

  /**
   * Describes groups of both protocols by group id.
   *
   * Each id is described through ConsumerGroupDescribe where the broker supports it, and through
   * DescribeGroups otherwise, so classic and consumer groups come back in one shape. Rows are not
   * returned in the order the ids were given.
   *
   * @param {string[]} groupIds
   * @param {object} [options] additive call budget and authorization mask flag
   * @param {boolean} [options.includeAuthorizedOperations=false] request raw signed masks
   * @returns {Promise<{ groups: object[] }>}
   * @throws {Error} original routing/protocol failure or call deadline
   */
  const describeGroups = async (groupIds, options = {}) =>
    createGroupDescriptions({ cluster, retry, logger: rootLogger }).describe(groupIds, options)

  /**
   * Delete groups, each through its own group coordinator.
   *
   * A broker that speaks DeleteGroups v3 answers a message per group. It is kept as `errorMessage`
   * (`null` when the broker sent none, `''` when it sent an empty one) on each result and on each
   * failed group of the `KafkaJSDeleteGroupsError` that rejects the call; older brokers omit the
   * property. A group that failed rejects the call once, with every failed group, and is not
   * retried.
   *
   * Every coordinator's request settles before a retry. When one coordinator is stale or
   * unavailable, the retry resends only the groups no coordinator has answered, so a group that was
   * deleted is never sent again, and its row is kept for the result. Rows come back by attempt,
   * then by coordinator.
   *
   * @param {string[]} [groupIds]
   * @return {Promise<DeleteGroups>}
   * @throws {KafkaJSDeleteGroupsError} `groups` holds `{ groupId, errorCode, error, errorMessage? }`
   *   for each group that failed
   *
   * @typedef {Array} DeleteGroups
   * @property {string} groupId
   * @property {number} errorCode
   * @property {string|null} [errorMessage] the broker's message for the group (DeleteGroups v3+)
   */
  const deleteGroups = async (groupIds) => {
    if (!groupIds || !Array.isArray(groupIds)) {
      throw new KafkaJSNonRetriableError(`Invalid groupIds array ${groupIds}`)
    }

    const invalidGroupId = groupIds.find((g) => typeof g !== 'string')

    if (groupIds.some((g) => typeof g !== 'string')) {
      throw new KafkaJSNonRetriableError(`Invalid groupId name: ${JSON.stringify(invalidGroupId)}`)
    }

    const retrier = retrierOf(retry)
    const requested = groupIds.slice()
    const rowsByNode = {}
    const answered = new Set()

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        const pending = requested.filter((groupId) => !answered.has(groupId))

        if (pending.length > 0) {
          checked(await waitOf(async () => cluster.refreshMetadata()))

          const brokersPerGroups = {}
          const brokersPerNode = {}
          for (const groupId of pending) {
            const broker = checked(
              await waitOf(async () => cluster.findGroupCoordinator({ groupId }))
            )
            if (brokersPerGroups[broker.nodeId] === undefined) brokersPerGroups[broker.nodeId] = []
            brokersPerGroups[broker.nodeId].push(groupId)
            brokersPerNode[broker.nodeId] = broker
          }

          const nodeIds = Object.keys(brokersPerNode)
          checked(
            await waitOf(() =>
              settleMutations(
                nodeIds.map(async (nodeId) =>
                  brokersPerNode[nodeId].deleteGroups(brokersPerGroups[nodeId])
                ),
                (value, index) => {
                  const nodeId = nodeIds[index]
                  rowsByNode[nodeId] = [...(rowsByNode[nodeId] || []), ...value.results]
                  for (const groupId of brokersPerGroups[nodeId]) answered.add(groupId)
                }
              )
            )
          )
        }

        // Rows keep coordinator order, as a single attempt returns them, whichever attempt answered.
        const results = Object.keys(rowsByNode).flatMap((nodeId) => rowsByNode[nodeId])
        const errors = results
          .map(({ groupId, errorCode, error, errorMessage }) => ({
            groupId,
            errorCode,
            error,
            ...(errorMessage !== undefined && { errorMessage }),
          }))
          .filter(({ errorCode }) => errorCode !== 0)

        if (errors.length > 0) throw new KafkaJSDeleteGroupsError('Error in DeleteGroups', errors)

        return results
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER' || isStaleCoordinator(e)) {
          logger.warn('Could not delete groups', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * The members of a group as LeaveGroup names them: a static member by its instance id alone (an
   * empty member id), a dynamic one by its member id. Groups of both protocols are described.
   * @param {string} groupId
   * @returns {Promise<{ memberId: string, groupInstanceId?: string }[]>} empty for a group that does
   *   not exist or has no member
   * @throws {KafkaJSProtocolError} when the group's row carries an error other than "not found"
   */
  const describedMembers = async (groupId) => {
    const {
      groups: [group],
    } = checked(await waitOf(async () => describeGroups([groupId])))

    if (group === undefined) {
      throw new KafkaJSNonRetriableError(`The coordinator described no group for ${groupId}`)
    }

    if (group.errorCode !== 0 && group.errorCode !== GROUP_ID_NOT_FOUND_CODE) {
      throw createErrorFromCode(group.errorCode)
    }

    if (group.protocolType !== CONSUMER_PROTOCOL_TYPE && group.protocolType !== '') {
      throw new KafkaJSNonRetriableError(
        `Group ${groupId} is not a consumer group (its protocol type is "${group.protocolType}")`
      )
    }

    return group.members.map(({ memberId, groupInstanceId }) =>
      groupInstanceId == null ? { memberId } : { memberId: '', groupInstanceId }
    )
  }

  /**
   * Removes members from a consumer group, for instances that are already gone (KIP-345). A member
   * that is still running rejoins on its next heartbeat, so this is not a way to stop a consumer.
   *
   * With `members` omitted, every member `describeGroups` reports is removed, static ones by
   * instance id and dynamic ones by member id, and a group with no member resolves `[]` without a
   * request; a group of another protocol type (a Connect group, say) is refused, as Java does.
   * With `members` given, each `{ groupInstanceId }` names a static member; a repeated
   * instance id is sent once. The request goes to the group coordinator. A coordinator lookup that
   * fails, and a coordinator that answers it is still loading the group, are tried again on the
   * `retry` budget; any other top-level answer rejects, and a written request with no answer
   * rejects with `KafkaJSUnknownOutcomeError`. A member that could not be removed is a result, not
   * an error: a group that does not exist answers every member `UNKNOWN_MEMBER_ID`.
   *
   * @param {Object} options
   * @param {string} options.groupId
   * @param {{ groupInstanceId: string }[]} [options.members]
   * @param {string} [options.reason='member was removed by an admin'] logged by the coordinator,
   *   cut to 255 characters and sent only to a broker that offers LeaveGroup v5; `null` and the
   *   empty string mean the default, as in Java
   * @returns {Promise<RemoveMemberResult[]>} one result per member sent
   * @throws {KafkaJSNonRetriableError} for a `groupId` that is not a non-empty string, an empty
   *   `members`, a `groupInstanceId` that is blank, a `reason` that is neither a string nor null,
   *   or (without `members`) a group that is not a consumer group
   * @throws {KafkaJSNumberOfRetriesExceeded} when the coordinator never settles
   *
   * @typedef {Object} RemoveMemberResult
   * @property {string} memberId the member id sent, empty for a static member named by instance id
   * @property {string|null} groupInstanceId
   * @property {number} errorCode 0 when the member was removed
   * @property {KafkaJSProtocolError|null} error the error for `errorCode`, or null
   */
  const removeMembersFromConsumerGroup = async ({ groupId, members, reason } = {}) => {
    if (typeof groupId !== 'string' || groupId.length === 0) {
      throw new KafkaJSNonRetriableError(`Invalid groupId ${groupId}`)
    }

    if (reason != null && typeof reason !== 'string') {
      throw new KafkaJSNonRetriableError(
        `Invalid reason ${JSON.stringify(reason)}, expected a string`
      )
    }

    const leaving =
      members === undefined
        ? checked(await waitOf(async () => describedMembers(groupId)))
        : staticMembers(members)

    if (leaving.length === 0) {
      return []
    }

    const sentReason = (reason == null || reason === '' ? DEFAULT_REMOVE_REASON : reason).slice(
      0,
      MAX_REMOVE_REASON_LENGTH
    )
    const request = {
      groupId,
      members: leaving.map((member) => ({ ...member, reason: sentReason })),
      [ADMIN_MUTATION]: ADMIN_MUTATION,
    }

    return retrierOf(retry)(async (bail, retryCount, retryTime) => {
      try {
        const coordinator = checked(
          await waitOf(async () => cluster.findGroupCoordinator({ groupId }))
        )
        const response = checked(await waitOf(async () => coordinator.leaveGroup(request)))

        return leaving.map((member, index) => toRemoveMemberResult(member, response.members[index]))
      } catch (e) {
        if (endsCall(e) || isAnsweredError(e)) return bail(e)
        if (isStaleCoordinator(e) || e.code === COORDINATOR_LOAD_IN_PROGRESS_CODE) {
          logger.warn('Could not remove members from the group', {
            error: e.message,
            retryCount,
            retryTime,
          })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * Delete topic records up to the selected partition offsets.
   *
   * The topic's offsets are read first, through a refresh that names the topic, which waits for a
   * partition without a leader within the `retry` budget and then fails. The partitions' leaders
   * are looked up next, inside the retrier: a cached leader of -1 refreshes the metadata and goes
   * round again, as the Java `Admin#deleteRecords` retries its leader lookup, and so does any
   * retriable failure before a request is written. The request to each leader is built on the
   * first attempt that finds them all and kept: a leader that answered, with or without partition
   * errors, is not asked again, and a retry goes only to the leaders whose request was not sent,
   * at the node it was built for. A written request that got no answer rejects the call with
   * `KafkaJSUnknownOutcomeError`, which wins over partition errors from other leaders.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/admin/internals/PartitionLeaderStrategy.java
   *
   * The public call checks and copies its input (`deletionInput`) synchronously, when it is made,
   * so a change the caller makes to the input afterwards reaches no request.
   *
   * @param {string} topic
   * @param {Array<SeekEntry>} partitions
   * @return {Promise}
   * @throws {KafkaJSNonRetriableError} before any request, when the partitions are not a
   *   non-empty array of entries with a partition id listed once and an offset from 0 to 2^63 - 1
   *   or `-1`
   * @throws {KafkaJSDeleteTopicRecordsError} at once, not retried, when a requested partition is
   *   not in the topic's metadata, once the offsets read has succeeded: a topic that stays
   *   leaderless fails that read first, with the `KafkaJSNonRetriableError` below
   * @throws {KafkaJSNumberOfRetriesExceeded} when a failure before a request is written outlives
   *   the `retry` budget; its `cause` is the last failure
   * @throws {KafkaJSNonRetriableError} once every leader answered, when any answered with
   *   partition errors (its `cause` is one `KafkaJSDeleteTopicRecordsError` naming the refused
   *   partitions of all leaders), or when the offsets read fails, as for a partition that stays
   *   without a leader (`type` `LEADER_NOT_AVAILABLE`)
   * @throws {KafkaJSUnknownOutcomeError} when a written request got no answer
   *
   * @typedef {Object} SeekEntry
   * @property {number} partition
   * @property {string} offset
   */
  const deleteTopicRecords = async ({ topic, partitions }) => {
    const topicOffsets = checked(await waitOf(async () => fetchTopicOffsets(topic)))
    /** @type {Object<string, { topic: string, partitions: SeekEntry[] }>|null} */
    let seekEntriesByBroker = null
    /** @type {{ partition: number, offset: string, error: Error }[]} answered partition errors */
    const refused = []

    const retrier = retrierOf(retry)
    return retrier(async (bail) => {
      try {
        if (seekEntriesByBroker === null) {
          const partitionsByBroker = cluster.findLeaderForPartitions(
            topic,
            partitions.map((p) => p.partition)
          )

          const partitionsFound = values(partitionsByBroker).flat()
          const leaderNotFoundErrors = []
          partitions.forEach(({ partition, offset }) => {
            if (!partitionsFound.includes(partition)) {
              leaderNotFoundErrors.push({
                partition,
                offset,
                error: new KafkaJSBrokerNotFound('Could not find the leader for the partition', {
                  retriable: false,
                }),
              })
              return
            }
            const { low } = topicOffsets.find((p) => p.partition === partition) || {
              high: undefined,
              low: undefined,
            }
            // warn in case of offset below low watermark
            if (offset !== '-1' && low !== undefined && BigInt(offset) < BigInt(low)) {
              logger.warn(
                'The requested offset is before the earliest offset maintained on the partition - no records will be deleted from this partition',
                {
                  topic,
                  partition,
                  offset,
                }
              )
            }
          })

          if (leaderNotFoundErrors.length > 0) {
            return bail(
              new KafkaJSDeleteTopicRecordsError({ topic, partitions: leaderNotFoundErrors })
            )
          }

          seekEntriesByBroker = entries(partitionsByBroker).reduce(
            (obj, [nodeId, nodePartitions]) => {
              obj[nodeId] = {
                topic,
                partitions: partitions.filter((p) => nodePartitions.includes(p.partition)),
              }
              return obj
            },
            {}
          )
        }

        const brokerRequests = entries(seekEntriesByBroker).map(
          ([nodeId, { topic, partitions }]) =>
            async () => {
              const broker = checked(await waitOf(async () => cluster.findBroker({ nodeId })))
              try {
                checked(
                  await waitOf(async () =>
                    broker.deleteRecords({ topics: [{ topic, partitions }] })
                  )
                )
              } catch (e) {
                if (!(e instanceof KafkaJSDeleteTopicRecordsError)) throw e
                refused.push(...e.partitions)
              }
              // an answered leader is not asked again
              delete seekEntriesByBroker[nodeId]
            }
        )

        checked(await waitOf(() => settleMutations(brokerRequests.map((request) => request()))))
      } catch (e) {
        if (endsCall(e) || isAnsweredError(e)) return bail(e)
        if (e.retriable && staleMetadata(e)) checked(await waitOf(() => cluster.refreshMetadata()))
        throw e
      }
      if (refused.length > 0) {
        const error = new KafkaJSDeleteTopicRecordsError({ topic, partitions: refused })
        return bail(new KafkaJSNonRetriableError(error, { cause: error }))
      }
    })
  }

  /**
   * @param {Array<ACLEntry>} acl
   * @return {Promise<void>}
   *
   * @typedef {Object} ACLEntry
   */
  const createAcls = async ({ acl }) => {
    if (!acl || !Array.isArray(acl)) {
      throw new KafkaJSNonRetriableError(`Invalid ACL array ${acl}`)
    }
    if (acl.length === 0) {
      throw new KafkaJSNonRetriableError('Empty ACL array')
    }

    // Validate principal

    // Validate principal
    if (acl.some(({ principal }) => typeof principal !== 'string')) {
      throw new KafkaJSNonRetriableError(
        'Invalid ACL array, the principals have to be a valid string'
      )
    }

    // Validate host

    // Validate host
    if (acl.some(({ host }) => typeof host !== 'string')) {
      throw new KafkaJSNonRetriableError('Invalid ACL array, the hosts have to be a valid string')
    }

    // Validate resourceName

    // Validate resourceName
    if (acl.some(({ resourceName }) => typeof resourceName !== 'string')) {
      throw new KafkaJSNonRetriableError(
        'Invalid ACL array, the resourceNames have to be a valid string'
      )
    }

    let invalidType
    // Validate operation
    const validOperationTypes = Object.values(ACL_OPERATION_TYPES)
    invalidType = acl.find((i) => !validOperationTypes.includes(i.operation))

    if (invalidType) {
      throw new KafkaJSNonRetriableError(
        `Invalid operation type ${invalidType.operation}: ${JSON.stringify(invalidType)}`
      )
    }

    // Validate resourcePatternTypes

    // Validate resourcePatternTypes
    const validResourcePatternTypes = Object.values(RESOURCE_PATTERN_TYPES)
    invalidType = acl.find((i) => !validResourcePatternTypes.includes(i.resourcePatternType))

    if (invalidType) {
      throw new KafkaJSNonRetriableError(
        `Invalid resource pattern type ${invalidType.resourcePatternType}: ${JSON.stringify(
          invalidType
        )}`
      )
    }

    // Validate permissionTypes

    // Validate permissionTypes
    const validPermissionTypes = Object.values(ACL_PERMISSION_TYPES)
    invalidType = acl.find((i) => !validPermissionTypes.includes(i.permissionType))

    if (invalidType) {
      throw new KafkaJSNonRetriableError(
        `Invalid permission type ${invalidType.permissionType}: ${JSON.stringify(invalidType)}`
      )
    }

    // Validate resourceTypes
    const validResourceTypes = Object.values(ACL_RESOURCE_TYPES)
    invalidType = acl.find((i) => !validResourceTypes.includes(i.resourceType))

    if (invalidType) {
      throw new KafkaJSNonRetriableError(
        `Invalid resource type ${invalidType.resourceType}: ${JSON.stringify(invalidType)}`
      )
    }

    const retrier = retrierOf(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        checked(await waitOf(async () => cluster.refreshMetadata()))
        const broker = checked(await waitOf(async () => cluster.findControllerBroker()))
        checked(await waitOf(async () => broker.createAcls({ acl })))

        return true
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not create ACL', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * @param {ACLResourceTypes} resourceType The type of resource
   * @param {string} resourceName The name of the resource
   * @param {ACLResourcePatternTypes} resourcePatternType The resource pattern type filter
   * @param {string} principal The principal name
   * @param {string} host The hostname
   * @param {ACLOperationTypes} operation The type of operation
   * @param {ACLPermissionTypes} permissionType The type of permission
   * @return {Promise<void>}
   *
   * @typedef {number} ACLResourceTypes
   * @typedef {number} ACLResourcePatternTypes
   * @typedef {number} ACLOperationTypes
   * @typedef {number} ACLPermissionTypes
   */
  const describeAcls = async ({
    resourceType,
    resourceName,
    resourcePatternType,
    principal,
    host,
    operation,
    permissionType,
  }) => {
    // Validate principal
    if (typeof principal !== 'string' && typeof principal !== 'undefined') {
      throw new KafkaJSNonRetriableError(
        'Invalid principal, the principal have to be a valid string'
      )
    }

    // Validate host

    // Validate host
    if (typeof host !== 'string' && typeof host !== 'undefined') {
      throw new KafkaJSNonRetriableError('Invalid host, the host have to be a valid string')
    }

    // Validate resourceName

    // Validate resourceName
    if (typeof resourceName !== 'string' && typeof resourceName !== 'undefined') {
      throw new KafkaJSNonRetriableError(
        'Invalid resourceName, the resourceName have to be a valid string'
      )
    }

    // Validate operation
    const validOperationTypes = Object.values(ACL_OPERATION_TYPES)
    if (!validOperationTypes.includes(operation)) {
      throw new KafkaJSNonRetriableError(`Invalid operation type ${operation}`)
    }

    // Validate resourcePatternType

    // Validate resourcePatternType
    const validResourcePatternTypes = Object.values(RESOURCE_PATTERN_TYPES)
    if (!validResourcePatternTypes.includes(resourcePatternType)) {
      throw new KafkaJSNonRetriableError(
        `Invalid resource pattern filter type ${resourcePatternType}`
      )
    }

    // Validate permissionType

    // Validate permissionType
    const validPermissionTypes = Object.values(ACL_PERMISSION_TYPES)
    if (!validPermissionTypes.includes(permissionType)) {
      throw new KafkaJSNonRetriableError(`Invalid permission type ${permissionType}`)
    }

    // Validate resourceType

    // Validate resourceType
    const validResourceTypes = Object.values(ACL_RESOURCE_TYPES)
    if (!validResourceTypes.includes(resourceType)) {
      throw new KafkaJSNonRetriableError(`Invalid resource type ${resourceType}`)
    }

    const retrier = retrierOf(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        checked(await waitOf(async () => cluster.refreshMetadata()))
        const broker = checked(await waitOf(async () => cluster.findControllerBroker()))
        const { resources } = checked(
          await waitOf(async () =>
            broker.describeAcls({
              resourceType,
              resourceName,
              resourcePatternType,
              principal,
              host,
              operation,
              permissionType,
            })
          )
        )
        return { resources }
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not describe ACL', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * @param {Array<ACLFilter>} filters
   * @return {Promise<void>}
   *
   * @typedef {Object} ACLFilter
   */
  const deleteAcls = async ({ filters }) => {
    if (!filters || !Array.isArray(filters)) {
      throw new KafkaJSNonRetriableError(`Invalid ACL Filter array ${filters}`)
    }

    if (filters.length === 0) {
      throw new KafkaJSNonRetriableError('Empty ACL Filter array')
    }

    // Validate principal

    // Validate principal
    if (
      filters.some(
        ({ principal }) => typeof principal !== 'string' && typeof principal !== 'undefined'
      )
    ) {
      throw new KafkaJSNonRetriableError(
        'Invalid ACL Filter array, the principals have to be a valid string'
      )
    }

    // Validate host

    // Validate host
    if (filters.some(({ host }) => typeof host !== 'string' && typeof host !== 'undefined')) {
      throw new KafkaJSNonRetriableError(
        'Invalid ACL Filter array, the hosts have to be a valid string'
      )
    }

    // Validate resourceName

    // Validate resourceName
    if (
      filters.some(
        ({ resourceName }) =>
          typeof resourceName !== 'string' && typeof resourceName !== 'undefined'
      )
    ) {
      throw new KafkaJSNonRetriableError(
        'Invalid ACL Filter array, the resourceNames have to be a valid string'
      )
    }

    let invalidType
    // Validate operation
    const validOperationTypes = Object.values(ACL_OPERATION_TYPES)
    invalidType = filters.find((i) => !validOperationTypes.includes(i.operation))

    if (invalidType) {
      throw new KafkaJSNonRetriableError(
        `Invalid operation type ${invalidType.operation}: ${JSON.stringify(invalidType)}`
      )
    }

    // Validate resourcePatternTypes

    // Validate resourcePatternTypes
    const validResourcePatternTypes = Object.values(RESOURCE_PATTERN_TYPES)
    invalidType = filters.find((i) => !validResourcePatternTypes.includes(i.resourcePatternType))

    if (invalidType) {
      throw new KafkaJSNonRetriableError(
        `Invalid resource pattern type ${invalidType.resourcePatternType}: ${JSON.stringify(
          invalidType
        )}`
      )
    }

    // Validate permissionTypes

    // Validate permissionTypes
    const validPermissionTypes = Object.values(ACL_PERMISSION_TYPES)
    invalidType = filters.find((i) => !validPermissionTypes.includes(i.permissionType))

    if (invalidType) {
      throw new KafkaJSNonRetriableError(
        `Invalid permission type ${invalidType.permissionType}: ${JSON.stringify(invalidType)}`
      )
    }

    // Validate resourceTypes
    const validResourceTypes = Object.values(ACL_RESOURCE_TYPES)
    invalidType = filters.find((i) => !validResourceTypes.includes(i.resourceType))

    if (invalidType) {
      throw new KafkaJSNonRetriableError(
        `Invalid resource type ${invalidType.resourceType}: ${JSON.stringify(invalidType)}`
      )
    }

    const retrier = retrierOf(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        checked(await waitOf(async () => cluster.refreshMetadata()))
        const broker = checked(await waitOf(async () => cluster.findControllerBroker()))
        const { filterResponses } = checked(
          await waitOf(async () => broker.deleteAcls({ filters }))
        )
        return { filterResponses }
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not delete ACL', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * Alter the replicas partitions are assigned to for a topic. The request is written at most
   * once: a controller lookup that fails before it (`NOT_CONTROLLER`) is retried, and a broker
   * answer, including per-partition errors, ends the call.
   * @param {Object} request
   * @param {import("../../types").IPartitionReassignment[]} request.topics topics and the paritions to be reassigned
   * @param {number} [request.timeout]
   * @returns {Promise<void>}
   * @throws {KafkaJSNonRetriableError} before any request, for topics that `reassignmentInput`
   *   refuses; the public call copies the topics when it is made, so a later change by the caller
   *   reaches no request
   * @throws {KafkaJSAggregateError} the broker's per-partition errors, one child per partition
   * @throws {KafkaJSUnknownOutcomeError} when the written request got no answer
   * @throws {KafkaJSNumberOfRetriesExceeded|Error} what ended the retries before the request was
   *   written, unchanged
   */
  const alterPartitionReassignments = async ({ topics, timeout, allowReplicationFactorChange }) => {
    const sent = topics.filter(({ partitionAssignment }) => partitionAssignment.length > 0)
    checked(
      await waitOf(async () =>
        retrierOf(retry)(async (bail, retryCount, retryTime) => {
          try {
            checked(await waitOf(async () => cluster.refreshMetadata()))
            const broker = checked(await waitOf(async () => cluster.findControllerBroker()))
            checked(
              await waitOf(async () =>
                broker.alterPartitionReassignments({
                  topics: sent,
                  timeout,
                  allowReplicationFactorChange,
                })
              )
            )
          } catch (e) {
            if (endsCall(e) || isAnsweredError(e) || isPerItemFailure(e)) return bail(e)
            if (e.type === 'NOT_CONTROLLER') {
              logger.warn('Could not reassign partitions', {
                error: e.message,
                retryCount,
                retryTime,
              })
              throw e
            }

            bail(e)
          }
        })
      )
    )
  }

  /**
   * List the partition reassignments in progress.
   * If a partition is not going through a reassignment, its AddingReplicas and RemovingReplicas fields will simply be empty.
   * If a partition doesn't exist, no response will be returned for it.
   * @param {Object} request
   * @param {import("../../types").TopicPartitions[]} request.topics topics and the paritions to be returned, if this is null will return all the topics.
   * @param {number} [request.timeout]
   * @returns {Promise<import("../../types").ListPartitionReassignmentsResponse>}
   */
  const listPartitionReassignments = async ({ topics = null, timeout }) => {
    if (topics) {
      if (!Array.isArray(topics)) {
        throw new KafkaJSNonRetriableError(`Invalid topics array ${topics}`)
      }

      if (topics.filter(({ topic }) => typeof topic !== 'string').length > 0) {
        throw new KafkaJSNonRetriableError(
          'Invalid topics array, the topic names have to be a valid string'
        )
      }

      const topicNames = new Set(topics.map(({ topic }) => topic))
      if (topicNames.size < topics.length) {
        throw new KafkaJSNonRetriableError(
          'Invalid topics array, it cannot have multiple entries for the same topic'
        )
      }

      for (const { topic, partitions } of topics) {
        if (!partitions || !Array.isArray(partitions)) {
          throw new KafkaJSNonRetriableError(
            `Invalid partition array: ${partitions} for topic: ${topic}`
          )
        }

        if (
          partitions.filter((partition) => typeof partition !== 'number' || partition < 0).length >=
          1
        ) {
          throw new KafkaJSNonRetriableError(
            `Invalid partition array: ${partitions} for topic: ${topic}. The partition indices have to be a valid number greater than 0.`
          )
        }
      }
    }

    const retrier = retrierOf(retry)

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        checked(await waitOf(async () => cluster.refreshMetadata()))
        const broker = checked(await waitOf(async () => cluster.findControllerBroker()))
        const response = checked(
          await waitOf(async () => broker.listPartitionReassignments({ topics, timeout }))
        )

        return { topics: response.topics }
      } catch (e) {
        if (e.type === 'NOT_CONTROLLER') {
          logger.warn('Could not reassign partitions', { error: e.message, retryCount, retryTime })
          throw e
        }

        bail(e)
      }
    })
  }

  /** @type {import("../../types").Admin["on"]} */
  const on = (eventName, listener) => {
    if (!eventNames.includes(eventName)) {
      throw new KafkaJSNonRetriableError(`Event name should be one of ${eventKeys}`)
    }

    return instrumentationEmitter.addListener(unwrapEvent(eventName), (event) => {
      event.type = wrapEvent(event.type)
      Promise.resolve(listener(event)).catch((e) => {
        logger.error(`Failed to execute listener: ${describeThrown(e)}`, {
          eventName,
          stack: describeThrown(e, 'stack'),
        })
      })
    })
  }

  /**
   * @return {Object} logger
   */
  const getLogger = () => logger

  return wrapAdmin(
    {
      ...createTransactions({ cluster, retry }),
      ...telemetry?.methods(),
      connect,
      disconnect,
      listTopics,
      createTopics,
      deleteTopics,
      createPartitions,
      fetchTopicMetadata,
      ...createClusterAdmin({ cluster, retry }),
      ...createConfigResources({ cluster, retry }),
      ...createUnregisterController({ cluster, retry }),
      events,
      ...createOffsets({ cluster, retry }),
      ...createLogDirs({ cluster, retry }),
      fetchOffsets,
      fetchTopicOffsets,
      fetchTopicOffsetsByTimestamp,
      setOffsets,
      resetOffsets,
      describeConfigs,
      alterConfigs,
      incrementalAlterConfigs,
      on,
      logger: getLogger,
      listGroups,
      describeGroups,
      describeShareGroups: shareGroups.describeShareGroups.bind(shareGroups),
      listShareGroupOffsets: shareGroups.listShareGroupOffsets.bind(shareGroups),
      alterShareGroupOffsets: shareGroups.alterShareGroupOffsets.bind(shareGroups),
      deleteShareGroupOffsets: shareGroups.deleteShareGroupOffsets.bind(shareGroups),
      deleteGroups,
      removeMembersFromConsumerGroup,
      describeAcls,
      deleteAcls,
      createAcls,
      deleteTopicRecords,
      alterPartitionReassignments,
      listPartitionReassignments,
      ...createClientQuotas({ cluster, retry, logger }),
      ...createScramCredentials({ cluster, retry, logger }),
      ...createDelegationTokens({ cluster, retry, logger }),
    },
    {
      describeProducers: 0,
      describeTransactions: 0,
      listTransactions: 0,
      abortTransaction: 0,
      fenceProducers: 0,
      listTopics: 0,
      createTopics: 0,
      deleteTopics: 0,
      createPartitions: 0,
      fetchTopicMetadata: 0,
      describeCluster: 0,
      deleteOffsets: 0,
      describeLogDirs: 0,
      alterReplicaLogDirs: 0,
      describeReplicaLogDirs: 0,
      fetchOffsetsForGroups: 0,
      electLeaders: 0,
      describeFeatures: 0,
      updateFeatures: 0,
      listConfigResources: 0,
      unregisterController: 0,
      fetchOffsets: 0,
      setOffsets: 0,
      resetOffsets: 0,
      describeConfigs: 0,
      alterConfigs: 0,
      incrementalAlterConfigs: 0,
      listGroups: 0,
      removeMembersFromConsumerGroup: 0,
      describeAcls: 0,
      deleteAcls: 0,
      createAcls: 0,
      deleteTopicRecords: 0,
      alterPartitionReassignments: 0,
      listPartitionReassignments: 0,
      describeClientQuotas: 0,
      alterClientQuotas: 0,
      describeUserScramCredentials: 0,
      alterUserScramCredentials: 0,
      createDelegationToken: 0,
      renewDelegationToken: 0,
      expireDelegationToken: 0,
      describeDelegationToken: 0,
      fetchTopicOffsets: 1,
      fetchTopicOffsetsByTimestamp: 2,
      describeGroups: 1,
      describeShareGroups: 1,
      listShareGroupOffsets: 0,
      alterShareGroupOffsets: 0,
      deleteShareGroupOffsets: 0,
      deleteGroups: 1,
    },
    apiTimeout,
    lifecycle,
    { deleteTopicRecords: deletionInput, alterPartitionReassignments: reassignmentInput }
  )
}
