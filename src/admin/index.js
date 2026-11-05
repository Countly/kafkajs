const createDelegationTokens = require('./delegationTokens')
const createClientQuotas = require('./clientQuotas')
const createScramCredentials = require('./scramCredentials')
const createTransactions = require('./transactions')
const createClusterAdmin = require('./clusterAdmin')
const createConfigResources = require('./configResources')
const { inspect } = require('util')
const {
  retrierOf,
  wrapAdmin,
  validateApiTimeout,
  checked,
  waitOf,
  sharedSetup,
  waitForOf: waitFor,
} = require('./deadline')
const { createOffsets } = require('./offsets')
const createLogDirs = require('./logDirs')
const groupBy = require('../utils/groupBy')
const createConsumer = require('../consumer')
const { disposeConsumer } = require('../consumer/disposal')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const { events, wrap: wrapEvent, unwrap: unwrapEvent } = require('./instrumentationEvents')
const { LEVELS } = require('../loggers')
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
const { createGroupDescriptions } = require('./groupDescriptions')
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
const CONSUMER_PROTOCOL_TYPE = 'consumer'
const UNANSWERED_MEMBER_CODE = -1
const THROTTLING_QUOTA_EXCEEDED_CODE = 89

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
    .sort()
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
 * The key of one partition of one topic, for the calls that answer per partition.
 * @param {string} topic
 * @param {number} partition
 * @returns {string}
 */
const partitionKey = (topic, partition) => `${topic}|${partition}`

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
 * @throws {KafkaJSNonRetriableError} when a resource repeats, `configEntries` is not an array, an
 *   entry fails {@link assertIncrementalEntry}, or a config name repeats in one resource whatever
 *   its operation
 */
const assertIncrementalEntries = (resources) => {
  const seenResources = new Set()

  for (const { type, name, configEntries } of resources) {
    const resourceKey = `${type}/${name}`

    if (seenResources.has(resourceKey)) {
      throw new KafkaJSNonRetriableError(
        `Invalid resources array, it cannot have multiple entries for the same resource: ${JSON.stringify({ type, name })}`
      )
    }
    seenResources.add(resourceKey)

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
 * resources. A resource the brokers did not answer gets an `UNKNOWN` result, never a success.
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

  return resources.map(
    ({ type, name }) =>
      answered.get(`${type}/${name}`) ?? {
        errorCode: -1,
        errorMessage: 'The broker did not answer for this resource',
        resourceType: type,
        resourceName: name,
      }
  )
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
}) => {
  apiTimeout = validateApiTimeout(apiTimeout)
  const logger = rootLogger.namespace('Admin')
  const instrumentationEmitter = rootInstrumentationEmitter || new InstrumentationEventEmitter()

  /**
   * @returns {Promise}
   */
  const connect = async () => {
    await sharedSetup(() => cluster.connect())
    instrumentationEmitter.emit(CONNECT)
  }

  /**
   * @return {Promise}
   */
  const disconnect = async () => {
    await sharedSetup(() => cluster.disconnect())
    instrumentationEmitter.emit(DISCONNECT)
  }

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

  /** Deletes names or canonical IDs with the existing void/throw contract.
   * ID responses must cover the attempted set before cleanup. Confirmed IDs leave the mutation
   * set before follow-up metadata refresh; metadata retries share the invocation deadline.
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
      let pending = ids
      const knownNames = new Map()
      const forget = (confirmed) => {
        for (const id of confirmed) {
          const name = knownNames.get(id)
          if (name !== undefined && cluster.metadataCache?.topicId(name) === id)
            cluster.targetTopics.delete(name)
        }
      }
      await retrierOf(retry)(async (bail) => {
        try {
          checked(await waitOf(() => cluster.refreshMetadata()))
          for (const id of pending) {
            const name = cluster.metadataCache?.topicName(id)
            if (name !== undefined && !knownNames.has(id)) knownNames.set(id, name)
          }
          const broker = checked(await waitOf(() => cluster.findControllerBroker()))
          checked(await waitOf(() => broker.deleteTopics({ topicIds: pending, timeout })))
          const confirmed = pending
          pending = []
          forget(confirmed)
        } catch (error) {
          const failures = error.topicErrors?.map((row) => ({
            ...row,
            topic: row.topic ?? row.topicId,
          }))
          if (failures) error.topicErrors = failures
          if (failures) {
            const failed = new Set(failures.map((row) => row.topicId))
            forget(pending.filter((id) => !failed.has(id)))
          }
          if (
            failures?.length > 0 &&
            failures.every((row) => row.errorCode === THROTTLING_QUOTA_EXCEEDED_CODE)
          ) {
            const refused = new Set(failures.map((row) => row.topicId))
            pending = pending.filter((id) => refused.has(id))
            if (pending.length === 0) bail(error)
            throw error
          }
          if (error.type === 'NOT_CONTROLLER') {
            checked(await waitOf(() => cluster.refreshMetadata()))
            throw error
          }
          bail(error)
        }
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
    if (!topics || !Array.isArray(topics)) {
      throw new KafkaJSNonRetriableError(`Invalid topics array ${topics}`)
    }

    if (topics.filter((topic) => typeof topic !== 'string').length > 0) {
      throw new KafkaJSNonRetriableError('Invalid topics array, the names must be a valid string')
    }

    const retrier = retrierOf(retry)
    let pending = topics

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        checked(await waitOf(async () => cluster.refreshMetadata()))
        const broker = checked(await waitOf(async () => cluster.findControllerBroker()))
        checked(await waitOf(async () => broker.deleteTopics({ topics: pending, timeout })))

        // Remove deleted topics

        // Remove deleted topics
        for (const topic of topics) {
          cluster.targetTopics.delete(topic)
        }

        checked(await waitOf(async () => cluster.refreshMetadata()))
      } catch (e) {
        if (e.topicErrors) {
          // a name the broker did not list as failed was deleted: the next refresh must not ask for it
          const failed = new Set(e.topicErrors.map(({ topic }) => topic))
          for (const topic of pending) {
            if (!failed.has(topic)) {
              cluster.targetTopics.delete(topic)
            }
          }
        }

        const refused = throttledTopics(e)

        if (refused) {
          pending = pending.filter((topic) => refused.has(topic))
          logger.warn('A quota refused topic deletions', {
            topics: [...refused],
            retryCount,
            retryTime,
          })
          throw e
        }

        if (['NOT_CONTROLLER', 'UNKNOWN_TOPIC_OR_PARTITION'].includes(e.type)) {
          logger.warn('Could not delete topics', { error: e.message, retryCount, retryTime })
          throw e
        }

        if (e.type === 'REQUEST_TIMED_OUT') {
          logger.error(
            'Could not delete topics, check if "delete.topic.enable" is set to "true" (the default value is "false") or increase the timeout',
            {
              error: e.message,
              retryCount,
              retryTime,
            }
          )
        }

        bail(e)
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
   * Commits the `-1` (latest) and `-2` (earliest) entries of a `setOffsets` call as the group's
   * offsets, as the admin's joined member. A consumer seek to a sentinel only moves that consumer's
   * position and commits nothing, so `setOffsets` commits the sentinels itself; the seeks commit the
   * concrete offsets. The last entry for a partition wins, as it does for the seeks, and an entry
   * for a partition the topic does not have is dropped, as a seek to one is.
   * @param {import('../../types').Consumer} consumer the admin's member of the group, joined
   * @param {{ groupId: string, topic: string, partitions: Array<SeekEntry> }} request
   * @returns {Promise<void>}
   * @throws {KafkaJSProtocolError} the coordinator's answer when it rejects the commit
   */
  const commitSentinels = async (consumer, { groupId, topic, partitions }) => {
    const last = new Map(partitions.map(({ partition, offset }) => [partition, Number(offset)]))
    const existing = new Set(
      cluster.findTopicPartitionMetadata(topic).map(({ partitionId }) => partitionId)
    )
    const sentinels = [...last]
      .filter(
        ([partition, offset]) =>
          existing.has(partition) && (offset === EARLIEST_OFFSET || offset === LATEST_OFFSET)
      )
      .map(([partition, offset]) => ({ partition, offset: String(offset) }))
    if (sentinels.length === 0) return

    const { generationId, memberId, groupInstanceId } = consumer.groupMetadata()
    const coordinator = checked(await waitOf(async () => cluster.findGroupCoordinator({ groupId })))
    checked(
      await waitOf(async () =>
        coordinator.offsetCommit({
          groupId,
          groupGenerationId: generationId,
          memberId,
          groupInstanceId,
          topics: [{ topic, partitions: sentinels }],
        })
      )
    )
  }

  /**
   * @typedef {Object} SeekEntry
   * @property {number} partition
   * @property {string} offset
   */

  /**
   * Sets the committed offsets of a consumer group that has no running members, through a
   * temporary member of the group. The last entry for a partition wins, and an entry for a
   * partition the topic does not have is ignored. An offset of `-1` (latest) or `-2` (earliest) is
   * committed as given and stays that marker until it is resolved (`fetchOffsets` with
   * `resolveOffsets` does so); any other offset is committed by a seek of the member. On every
   * exit the call awaits disposal of the member, which releases its cluster subscriptions even if
   * stopping fails. A disposal failure does not replace the call's outcome. Disposal itself does
   * not disconnect the admin's shared cluster.
   * @param {object} request
   * @param {string} request.groupId
   * @param {string} request.topic
   * @param {Array<SeekEntry>} request.partitions at least one entry
   * @returns {Promise<void>}
   * @throws {KafkaJSNonRetriableError} for a missing `groupId` or `topic`, no `partitions`, an
   *   entry with an invalid partition or offset, or a group whose state is not `Empty` or `Dead`
   * @throws {Error} what subscribing, describing the group, starting the member, committing a
   *   sentinel offset or stopping the member raised; a stop failure can replace a sentinel-commit
   *   failure
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

    return cluster.withGroup(groupId, async () => {
      const consumer = createConsumer({
        logger: rootLogger.namespace('Admin', LEVELS.NOTHING),
        cluster,
        groupId,
      })

      try {
        checked(await waitOf(async () => consumer.subscribe({ topic, fromBeginning: true })))
        const description = checked(await waitOf(async () => consumer.describeGroup()))

        if (!isConsumerGroupRunning(description)) {
          throw new KafkaJSNonRetriableError(
            `The consumer group must have no running instances, current state: ${description.state}`
          )
        }

        checked(
          await waitOf(
            async () =>
              new Promise((resolve, reject) => {
                let committed = null
                consumer.on(consumer.events.GROUP_JOIN, () => {
                  committed ??= commitSentinels(consumer, { groupId, topic, partitions })
                  committed.catch(() => {})
                })
                consumer.on(consumer.events.FETCH, () => {
                  committed
                    .finally(() => consumer.stop())
                    .then(resolve)
                    .catch(reject)
                })

                consumer
                  .run({
                    eachBatchAutoResolve: false,
                    eachBatch: async () => true,
                  })
                  .catch(reject)

                // This consumer doesn't need to consume any data
                consumer.pause([{ topic }])

                for (const seekData of partitions) {
                  consumer.seek({ topic, ...seekData })
                }
              })
          )
        )
      } finally {
        // The consumer shares this admin's cluster, so it is disposed, not disconnected. A failed
        // stop is already logged by the consumer and must not replace the call's own outcome.
        await sharedSetup(() => disposeConsumer(consumer).catch(() => {}))
      }
    })
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
          await waitOf(async () => Promise.all(brokers.map(alterConfigsAction)))
        )
        const responseResources = responses.reduce(
          (result, { resources }) => [...result, ...resources],
          []
        )

        return { resources: responseResources }
      } catch (e) {
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
          await waitOf(async () =>
            Promise.all(
              Array.from(resourcesByBroker, ([broker, group]) =>
                broker.incrementalAlterConfigs({ resources: group, validateOnly: !!validateOnly })
              )
            )
          )
        )
        const results = responses.flatMap((response) => response.resources)

        if (results.some(({ errorCode }) => errorCode === NOT_CONTROLLER_CODE)) {
          throw createErrorFromCode(NOT_CONTROLLER_CODE)
        }

        return { resources: inRequestOrder(resources, results) }
      } catch (e) {
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
   * The whole read, the metadata refresh and every attempt included, has one deadline of 60 s
   * inherited from the public call. At its actual monotonic deadline it rejects with
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
   * budget, as the other admin calls are, and the whole read has one deadline of 60 s, after which
   * it rejects with `KafkaJSTimeout`. A broker that does not offer the API is asked through
   * Metadata after all. The option applies only to topics named explicitly: empty or omitted `topics` go through Metadata.
   *
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas
   *
   * @param {Object} [options]
   * @param {string[]} [options.topics]
   * @param {boolean} [options.useDescribeTopicPartitions=false]
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

    const invalidGroupId = groupIds.some((g) => typeof g !== 'string')

    if (invalidGroupId) {
      throw new KafkaJSNonRetriableError(`Invalid groupId name: ${JSON.stringify(invalidGroupId)}`)
    }

    const retrier = retrierOf(retry)
    const results = []
    const answered = new Set()

    return retrier(async (bail, retryCount, retryTime) => {
      try {
        const pending = groupIds.filter((groupId) => !answered.has(groupId))

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
          const outcomes = checked(
            await Promise.allSettled(
              nodeIds.map(async (nodeId) =>
                checked(
                  await waitOf(async () =>
                    brokersPerNode[nodeId].deleteGroups(brokersPerGroups[nodeId])
                  )
                )
              )
            )
          )

          outcomes.forEach((outcome, index) => {
            if (outcome.status !== 'fulfilled') return
            for (const row of outcome.value.results) results.push(row)
            for (const groupId of brokersPerGroups[nodeIds[index]]) answered.add(groupId)
          })

          const rejected = outcomes.find(({ status }) => status === 'rejected')
          if (rejected) throw rejected.reason
        }

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
   * instance id is sent once. The request goes to the group coordinator, and a coordinator that is
   * not the right one, not available or still loading the group is asked again on the `retry`
   * budget; any other top-level error rejects. A member that could not be removed is a result, not
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
    }

    return retrierOf(retry)(async (bail, retryCount, retryTime) => {
      try {
        const coordinator = checked(
          await waitOf(async () => cluster.findGroupCoordinator({ groupId }))
        )
        const response = checked(await waitOf(async () => coordinator.leaveGroup(request)))

        return leaving.map((member, index) => toRemoveMemberResult(member, response.members[index]))
      } catch (e) {
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
   * retriable failure. The request to each leader is built on the first attempt that finds them all
   * and kept: a broker that already answered is not asked again, and a retry goes to the node it
   * was built for, after a stale-metadata answer too.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/admin/internals/PartitionLeaderStrategy.java
   *
   * @param {string} topic
   * @param {Array<SeekEntry>} partitions
   * @return {Promise}
   * @throws {KafkaJSDeleteTopicRecordsError} at once, not retried, when a requested partition is
   *   not in the topic's metadata, once the offsets read has succeeded: a topic that stays
   *   leaderless fails that read first, with the `KafkaJSNonRetriableError` below
   * @throws {KafkaJSNumberOfRetriesExceeded} when a retriable failure outlives the `retry` budget;
   *   its `cause` is the last failure
   * @throws {KafkaJSNonRetriableError} when a broker refuses a partition for good (its `cause` is
   *   the `KafkaJSDeleteTopicRecordsError` naming the partitions), or when the offsets read fails,
   *   as for a partition that stays without a leader (`type` `LEADER_NOT_AVAILABLE`)
   *
   * @typedef {Object} SeekEntry
   * @property {number} partition
   * @property {string} offset
   */
  const deleteTopicRecords = async ({ topic, partitions }) => {
    if (!topic || typeof topic !== 'string') {
      throw new KafkaJSNonRetriableError(`Invalid topic "${topic}"`)
    }

    if (!partitions || partitions.length === 0) {
      throw new KafkaJSNonRetriableError(`Invalid partitions`)
    }

    const topicOffsets = checked(await waitOf(async () => fetchTopicOffsets(topic)))
    /** @type {Object<string, { topic: string, partitions: SeekEntry[] }>|null} */
    let seekEntriesByBroker = null

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
            if (parseInt(offset) < parseInt(low) && parseInt(offset) !== -1) {
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

        const partitionErrors = []

        const brokerRequests = entries(seekEntriesByBroker).map(
          ([nodeId, { topic, partitions }]) =>
            async () => {
              const broker = checked(await waitOf(async () => cluster.findBroker({ nodeId })))
              checked(
                await waitOf(async () => broker.deleteRecords({ topics: [{ topic, partitions }] }))
              )
              // remove successful entry so it's ignored on retry
              delete seekEntriesByBroker[nodeId]
            }
        )

        checked(
          await waitOf(async () =>
            Promise.all(
              brokerRequests.map((request) =>
                request().catch((e) => {
                  if (e.name === 'KafkaJSDeleteTopicRecordsError') {
                    e.partitions.forEach(({ partition, offset, error }) => {
                      partitionErrors.push({
                        partition,
                        offset,
                        error,
                      })
                    })
                  } else {
                    // then it's an unknown error, not from the broker response
                    throw e
                  }
                })
              )
            )
          )
        )

        if (partitionErrors.length > 0) {
          throw new KafkaJSDeleteTopicRecordsError({
            topic,
            partitions: partitionErrors,
          })
        }
      } catch (e) {
        if (
          e.retriable &&
          (staleMetadata(e) ||
            e.partitions?.some(
              ({ error }) => staleMetadata(error) || error.name === 'KafkaJSMetadataNotLoaded'
            ))
        ) {
          checked(await waitOf(async () => cluster.refreshMetadata()))
        }
        throw e
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
   * Alter the replicas partitions are assigned to for a topic. A failure that names only some
   * partitions does not repeat the others: each retry sends just the partitions the broker said
   * could still go through (`retriable`), with their original replicas, and a partition it refused
   * for good is remembered across the retries.
   * @param {Object} request
   * @param {import("../../types").IPartitionReassignment[]} request.topics topics and the paritions to be reassigned
   * @param {number} [request.timeout]
   * @returns {Promise<void>}
   * @throws {KafkaJSAggregateError} when a partition was refused for good: one error per such
   *   partition, not retriable, followed by the failures of an attempt that ran out of retries
   * @throws {KafkaJSNumberOfRetriesExceeded|Error} what ended the retries, unchanged, when no
   *   partition was refused for good
   */
  const alterPartitionReassignments = async ({
    topics,
    timeout,
    allowReplicationFactorChange = true,
  }) => {
    if (typeof allowReplicationFactorChange !== 'boolean')
      throw new KafkaJSInvalidConfigurationError('Invalid allowReplicationFactorChange')
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

    for (const { topic, partitionAssignment } of topics) {
      if (!partitionAssignment || !Array.isArray(partitionAssignment)) {
        throw new KafkaJSNonRetriableError(
          `Invalid partitions array: ${partitionAssignment} for topic: ${topic}`
        )
      }

      for (const { partition, replicas } of partitionAssignment) {
        if (
          partition === null ||
          partition === undefined ||
          typeof partition !== 'number' ||
          partition < 0
        ) {
          throw new KafkaJSNonRetriableError(
            `Invalid partitions index: ${partition} for topic: ${topic}`
          )
        }

        if (!replicas || !Array.isArray(replicas)) {
          throw new KafkaJSNonRetriableError(
            `Invalid replica assignment: ${replicas} for topic: ${topic} on partition: ${partition}`
          )
        }

        if (replicas.filter((replica) => typeof replica !== 'number' || replica < 0).length >= 1) {
          throw new KafkaJSNonRetriableError(
            `Invalid replica assignment: ${replicas} for topic: ${topic} on partition: ${partition}. Replicas must be a non negative number`
          )
        }
      }
    }

    const outcome = new AggregateOutcome({
      keys: topics.flatMap(({ topic, partitionAssignment }) =>
        partitionAssignment.map(({ partition }) => partitionKey(topic, partition))
      ),
      keyOf: ({ topic, partition }) => partitionKey(topic, partition),
      select: (keys) =>
        topics
          .map((entry) => ({
            ...entry,
            partitionAssignment: entry.partitionAssignment.filter(({ partition }) =>
              keys.has(partitionKey(entry.topic, partition))
            ),
          }))
          .filter(({ partitionAssignment }) => partitionAssignment.length > 0),
      message: 'Errors altering partition reassignments',
    })
    const retrier = retrierOf(retry)
    let outstanding

    try {
      checked(
        await waitOf(async () =>
          retrier(async (bail, retryCount, retryTime) => {
            try {
              checked(await waitOf(async () => cluster.refreshMetadata()))
              const broker = checked(await waitOf(async () => cluster.findControllerBroker()))
              checked(
                await waitOf(async () =>
                  broker.alterPartitionReassignments({
                    topics: outcome.pending,
                    timeout,
                    allowReplicationFactorChange,
                  })
                )
              )
              outcome.succeeded()
            } catch (e) {
              if (e.type === 'NOT_CONTROLLER') {
                logger.warn('Could not reassign partitions', {
                  error: e.message,
                  retryCount,
                  retryTime,
                })
                throw e
              }

              if (isPerItemFailure(e)) {
                const retryable = outcome.settle(e)
                if (retryable) {
                  logger.warn('Could not reassign some partitions yet', {
                    partitions: retryable.errors.map(({ topic, partition }) =>
                      partitionKey(topic, partition)
                    ),
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

    outcome.finish(outstanding)
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
        logger.error(`Failed to execute listener: ${e.message}`, {
          eventName,
          stack: e.stack,
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
      connect,
      disconnect,
      listTopics,
      createTopics,
      deleteTopics,
      createPartitions,
      fetchTopicMetadata,
      ...createClusterAdmin({ cluster, retry }),
      ...createConfigResources({ cluster, retry }),
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
      deleteGroups: 1,
    },
    apiTimeout
  )
}
