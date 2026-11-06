const { requests } = require('../protocol/requests')
const { createErrorFromCode } = require('../protocol/error')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSProtocolError,
  KafkaJSShareGroupsNotSupportedError,
} = require('../errors')
const shareCapability = require('../consumer/share/shareCapability')
const { currentDeadline, checked, waitOf, retrierOf } = require('./deadline')
const { itemRetry } = require('./itemRetry')

/** @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java */
const COORDINATOR_LOAD_IN_PROGRESS = 14
const COORDINATOR_NOT_AVAILABLE = 15
const NOT_COORDINATOR = 16
const UNSUPPORTED_VERSION = 35
/** What the broker sends when authorized operations were not requested. */
const NO_AUTHORIZED_OPERATIONS = -2147483648
const MAX_INT32 = 2147483647
const MAX_INT64 = 9223372036854775807n
const DESCRIBE_KEY = 77
const LIST_OFFSETS_KEY = 90
const ALTER_OFFSETS_KEY = 91
const DELETE_OFFSETS_KEY = 92
const APIS = {
  [DESCRIBE_KEY]: 'ShareGroupDescribe',
  [LIST_OFFSETS_KEY]: 'DescribeShareGroupOffsets',
  [ALTER_OFFSETS_KEY]: 'AlterShareGroupOffsets',
  [DELETE_OFFSETS_KEY]: 'DeleteShareGroupOffsets',
}
const IMPLEMENTED = Object.fromEntries(
  Object.entries(APIS).map(([key, name]) => [key, requests[name]])
)

/**
 * Refuses caller input before any discovery or mutation.
 * @param {boolean} valid whether the input is acceptable
 * @param {string} message accepted domain and offending option
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} when `valid` is false
 */
const requireInput = (valid, message) => {
  if (!valid) throw new KafkaJSInvalidConfigurationError(message)
}

/**
 * Validates a nonempty identifier.
 * @param {any} value caller value
 * @param {string} label option name
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} for anything but a nonempty string
 */
const requireName = (value, label) =>
  requireInput(typeof value === 'string' && value.length > 0, `${label} must be a nonempty string`)

/**
 * Validates a canonical non-negative decimal int64 offset.
 * @param {any} offset caller value
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} for numbers, bigints, signs, fractions, leading zeroes and overflow
 */
const requireOffset = (offset) =>
  requireInput(
    typeof offset === 'string' &&
      /^(0|[1-9][0-9]*)$/.test(offset) &&
      offset.length <= 19 &&
      BigInt(offset) <= MAX_INT64,
    'offset must be a decimal string from 0 to 9223372036854775807'
  )

/**
 * Snapshots and validates a topic selection, detached from the caller's arrays before the first await.
 * Every array position is visited, so a hole is refused like any other undefined entry.
 * @param {any} topics caller selection; undefined means "all topics" for `list` only
 * @param {'list'|'alter'|'delete'} kind operation
 * @returns {object[]|null} wire selection; `null` only for an omitted `list` selection
 * @throws {KafkaJSInvalidConfigurationError} for invalid names, partitions, duplicates or offsets
 */
const selection = (topics, kind) => {
  if (kind === 'list' && topics === undefined) return null
  requireInput(Array.isArray(topics), 'topics must be an array')
  const names = new Set()
  return Array.from(topics, (row) => {
    const topic = kind === 'delete' ? row : row?.topic
    requireName(topic, 'topic')
    requireInput(!names.has(topic), 'topics must have distinct names')
    names.add(topic)
    if (kind === 'delete') return { topicName: topic }
    requireInput(Array.isArray(row.partitions), 'partitions must be an array')
    const partitions = new Set()
    return {
      topicName: topic,
      partitions: Array.from(row.partitions, (value) => {
        const partition = kind === 'alter' ? value?.partition : value
        requireInput(
          Number.isInteger(partition) && partition >= 0 && partition <= MAX_INT32,
          'partition must be an integer from 0 to 2147483647'
        )
        requireInput(!partitions.has(partition), 'partitions must be distinct')
        partitions.add(partition)
        if (kind === 'list') return partition
        requireOffset(value.offset)
        return { partitionIndex: partition, startOffset: value.offset }
      }),
    }
  })
}

/**
 * Whether a thrown error rejects the whole describe call; a coordinator-moved or loading error
 * is instead retried and, once the budget is spent, kept as that group's own row.
 * @param {Error} error failure of one describe attempt
 * @returns {boolean} false only for protocol codes 14, 15 and 16
 */
const isCallFailure = (error) =>
  ![COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR].includes(
    error.code ?? error.cause?.code
  )

/**
 * Finds a typed refusal or invariant failure on an error's cause chain.
 * @param {Error} error top-level failure, possibly wrapped by a retrier
 * @param {Function[]} types classes to look for
 * @returns {Error|null} the first matching instance, or null
 */
const causeOf = (error, types) => {
  const seen = new Set()
  for (let cause = error; cause && !seen.has(cause); cause = cause.cause) {
    if (types.some((type) => cause instanceof type)) return cause
    seen.add(cause)
  }
  return null
}

/**
 * The four share administration operations. Each call keeps its state in local maps and sets;
 * the class holds nothing between calls, and the admin factory supplies the single deadline wrapper.
 */
class ShareGroups {
  /**
   * @param {object} options collaborators
   * @param {object} options.cluster supplies `findGroupCoordinator` for the GROUP coordinator type
   * @param {object} options.retry retry settings shared with the other admin operations
   */
  constructor({ cluster, retry }) {
    this.cluster = cluster
    this.retry = retry
  }

  /**
   * Describes share groups, one request per coordinator, and returns rows in caller order.
   * Groups are looked up and batched by coordinator node. A repeated id shares one lookup and one
   * relocation allowance and comes back as a fresh row per repetition. Loading (14) is retried in
   * place under the retry budget; a moved coordinator (15/16) is looked up again once per id, after
   * which the broker's code stays on that group's row. Settled rows are never resent.
   * @param {string[]} groupIds nonempty string ids
   * @param {object} [options] public options; `apiTimeout` is consumed by the admin wrapper
   * @param {boolean} [options.includeAuthorizedOperations=false] request the raw signed mask
   * @returns {Promise<{groups: object[]}>} per-group failures are data on their own row
   * @throws {KafkaJSInvalidConfigurationError} for invalid input, before any lookup
   * @throws {KafkaJSShareGroupsNotSupportedError} when the coordinator cannot run the request
   * @throws {KafkaJSInvariantViolation} for a missing, foreign or duplicate answer row
   * @throws {Error} transport failure or the call's deadline
   */
  async describeShareGroups(groupIds, options = {}) {
    requireInput(
      options !== null && typeof options === 'object' && !Array.isArray(options),
      'options must be an object'
    )
    const { includeAuthorizedOperations = false } = options
    requireInput(Array.isArray(groupIds), 'groupIds must be an array')
    for (const id of groupIds) requireName(id, 'groupId')
    requireInput(
      typeof includeAuthorizedOperations === 'boolean',
      'includeAuthorizedOperations must be a boolean'
    )
    const ids = groupIds.slice()
    const routes = new Map()
    const relocated = new Set()
    const operation = currentDeadline()
    const unown = operation?.own(() => {
      routes.clear()
      relocated.clear()
    })
    try {
      const rows = await itemRetry(
        {
          items: [...new Set(ids)].map((groupId) => ({ groupId })),
          keyOf: (row) => row.groupId,
          retry: this.retry,
          retryable: (row) => row.retryCoordinator,
          callFailure: isCallFailure,
          sanitize: (message) => message,
        },
        (pending) =>
          this.#describeAttempt(pending, { includeAuthorizedOperations, routes, relocated })
      )
      checked(rows)
      const results = new Map(rows.map((row) => [row.groupId, row]))
      return {
        groups: ids.map((id) => this.#description(results.get(id), includeAuthorizedOperations)),
      }
    } catch (error) {
      currentDeadline()?.check()
      throw (
        causeOf(error, [KafkaJSShareGroupsNotSupportedError, KafkaJSInvariantViolation]) || error
      )
    } finally {
      unown?.()
      routes.clear()
      relocated.clear()
    }
  }

  /**
   * Lists the offsets and lag of a share group.
   * Omitting `topics` sends a null selection (every topic the group has offsets for); an explicit
   * empty array is sent as an empty selection. Decimal offsets and lag stay exact strings, and the
   * unknown-lag sentinel `-1` becomes `null`. Partition errors are data; a group error throws.
   * @param {object} options call options
   * @param {string} options.groupId nonempty group id
   * @param {Array<{topic: string, partitions: number[]}>} [options.topics] distinct topics, distinct int32 partitions
   * @returns {Promise<object[]>} `{ topic, topicId, partitions: [{ partition, startOffset, leaderEpoch, lag, errorCode, error? }] }`
   * @throws {KafkaJSInvalidConfigurationError} for invalid input, before any lookup
   * @throws {KafkaJSProtocolError} the group-level error, as the broker sent it
   * @throws {KafkaJSShareGroupsNotSupportedError} when the coordinator cannot run the request
   * @throws {Error} transport failure or the call's deadline
   */
  async listShareGroupOffsets(options = {}) {
    requireName(options?.groupId, 'groupId')
    const { groupId } = options
    const topics = selection(options.topics, 'list')
    const rows = await this.#offsets(groupId, LIST_OFFSETS_KEY, 'describeShareGroupOffsets', {
      groups: [{ groupId, topics }],
    })
    return rows.map((topic) => ({
      topic: topic.topicName,
      topicId: topic.topicId,
      partitions: topic.partitions.map((row) => ({
        partition: row.partitionIndex,
        startOffset: row.startOffset,
        leaderEpoch: row.leaderEpoch,
        lag: row.lag === '-1' ? null : row.lag,
        ...this.#outcome(row),
      })),
    }))
  }

  /**
   * Sets the start offsets of a group that has no members.
   * Every offset must be a canonical decimal string up to the int64 maximum; all input is checked
   * before the first lookup. If the call's deadline passes after the request was sent, whether the
   * broker applied it is unknown. Partition errors are data; a group error throws.
   * @param {object} options call options
   * @param {string} options.groupId nonempty group id
   * @param {Array<{topic: string, partitions: Array<{partition: number, offset: string}>}>} options.topics distinct topics and partitions
   * @returns {Promise<object[]>} `{ topic, topicId, partitions: [{ partition, errorCode, error? }] }`
   * @throws {KafkaJSInvalidConfigurationError} for invalid input, before any lookup
   * @throws {KafkaJSProtocolError} the group-level error, as the broker sent it
   * @throws {KafkaJSShareGroupsNotSupportedError} when the coordinator cannot run the request
   * @throws {Error} transport failure or the call's deadline
   */
  async alterShareGroupOffsets(options = {}) {
    requireName(options?.groupId, 'groupId')
    const { groupId } = options
    const topics = selection(options.topics, 'alter')
    const rows = await this.#offsets(groupId, ALTER_OFFSETS_KEY, 'alterShareGroupOffsets', {
      groupId,
      topics,
    })
    return rows.map((topic) => ({
      topic: topic.topicName,
      topicId: topic.topicId,
      partitions: topic.partitions.map((row) => ({
        partition: row.partitionIndex,
        ...this.#outcome(row),
      })),
    }))
  }

  /**
   * Deletes the offset state of the named topics for a group that no longer subscribes to them.
   * Topic errors, including a refusal for a topic the group still subscribes to, are data; a group
   * error throws.
   * @param {object} options call options
   * @param {string} options.groupId nonempty group id
   * @param {string[]} options.topics distinct nonempty topic names
   * @returns {Promise<object[]>} `{ topic, topicId, errorCode, error? }` per topic
   * @throws {KafkaJSInvalidConfigurationError} for invalid input, before any lookup
   * @throws {KafkaJSProtocolError} the group-level error, as the broker sent it
   * @throws {KafkaJSShareGroupsNotSupportedError} when the coordinator cannot run the request
   * @throws {Error} transport failure or the call's deadline
   */
  async deleteShareGroupOffsets(options = {}) {
    requireName(options?.groupId, 'groupId')
    const { groupId } = options
    const topics = selection(options.topics, 'delete')
    const rows = await this.#offsets(groupId, DELETE_OFFSETS_KEY, 'deleteShareGroupOffsets', {
      groupId,
      topics,
    })
    return rows.map((row) => ({
      topic: row.topicName,
      topicId: row.topicId,
      ...this.#outcome(row),
    }))
  }

  /**
   * @param {object} row wire row with a numeric `errorCode`
   * @returns {{errorCode: number, error?: KafkaJSProtocolError}} the code, plus its error when nonzero
   */
  #outcome(row) {
    return {
      errorCode: row.errorCode,
      ...(row.errorCode ? { error: createErrorFromCode(row.errorCode) } : {}),
    }
  }

  /**
   * Checks the selected coordinator's own API before a share frame is written. Advertisement,
   * range and finalized `share.version` are checked in that order; a finalized epoch of -1 leaves
   * the decision to the broker's runtime answer.
   * @param {object} broker connected coordinator with its advertised snapshot
   * @param {number} key the API about to be sent
   * @returns {void}
   * @throws {KafkaJSShareGroupsNotSupportedError} naming the refused API and stage
   */
  #require(broker, key) {
    const result = shareCapability({
      versions: broker.versions,
      finalizedFeatures: broker.finalizedFeatures,
      finalizedFeaturesEpoch: broker.finalizedFeaturesEpoch,
      implemented: IMPLEMENTED,
      required: [key],
    })
    if (result.ok) return
    throw new KafkaJSShareGroupsNotSupportedError(
      new Error(
        `${result.apiName}: ${result.reason}; client ${JSON.stringify(result.clientRange)}; broker ${JSON.stringify(result.brokerRange)}`
      ),
      result
    )
  }

  /**
   * Converts a runtime UNSUPPORTED_VERSION answer into the typed refusal; other codes pass.
   * @param {number} key the API that answered
   * @param {number} code protocol code on the envelope or item
   * @returns {void}
   * @throws {KafkaJSShareGroupsNotSupportedError} reason `refused-by-broker` for code 35
   */
  #refused(key, code) {
    if (code !== UNSUPPORTED_VERSION) return
    throw new KafkaJSShareGroupsNotSupportedError(createErrorFromCode(code), {
      apiKey: key,
      apiName: APIS[key],
      reason: 'refused-by-broker',
      shareVersion: null,
    })
  }

  /**
   * Finds the GROUP coordinator under the call's budget and checks it can run the API.
   * @param {string} groupId validated id
   * @param {number} key the API about to be sent
   * @returns {Promise<object>} the capable coordinator
   * @throws {KafkaJSShareGroupsNotSupportedError} when the coordinator refuses the API
   * @throws {Error} discovery failure or the call's deadline
   */
  async #locate(groupId, key) {
    const broker = checked(await waitOf(() => this.cluster.findGroupCoordinator({ groupId })))
    this.#require(broker, key)
    return broker
  }

  /**
   * Sends through the Broker method with the call's own guard, so the Broker refuses a frame the
   * deadline no longer allows even when transport timeouts are not enforced.
   * @param {object} broker selected coordinator
   * @param {string} method Broker method name
   * @param {object} request detached wire fields
   * @returns {Promise<object>} decoded response
   * @throws {Error} the deadline's refusal or a transport failure
   */
  async #send(broker, method, request) {
    const guard = currentDeadline()?.guard
    return checked(await waitOf(() => broker[method](request, { guard })))
  }

  /**
   * Runs one describe attempt over the still-pending ids, one request per coordinator node.
   * Rows asking to be retried carry `retryCoordinator`; a first 15/16 also drops that id's route.
   * @param {Array<{groupId: string}>} pending ids not yet settled
   * @param {object} context call-local state
   * @param {boolean} context.includeAuthorizedOperations wire flag
   * @param {Map<string, object>} context.routes coordinator per id
   * @param {Set<string>} context.relocated ids that already used their one relocation
   * @returns {Promise<object[]>} one row per pending id
   * @throws {KafkaJSInvariantViolation} for a missing, foreign or duplicate result row
   * @throws {Error} capability, transport or deadline failure
   */
  async #describeAttempt(pending, { includeAuthorizedOperations, routes, relocated }) {
    const batches = new Map()
    for (const { groupId } of pending) {
      let broker = routes.get(groupId)
      if (!broker) {
        broker = await this.#locate(groupId, DESCRIBE_KEY)
        routes.set(groupId, broker)
      }
      const batch = batches.get(broker.nodeId) || { broker, groupIds: [] }
      batch.groupIds.push(groupId)
      batches.set(broker.nodeId, batch)
    }
    const answers = await waitOf(() =>
      Promise.all(
        [...batches.values()].map(async ({ broker, groupIds }) => {
          const answer = await this.#send(broker, 'shareGroupDescribe', {
            groupIds,
            includeAuthorizedOperations,
          })
          return this.#correlate(answer, groupIds, { routes, relocated })
        })
      )
    )
    return checked(answers).flat()
  }

  /**
   * Matches one coordinator's answer to the ids it was asked about.
   * @param {object} answer decoded ShareGroupDescribe response
   * @param {string[]} groupIds ids sent in that request
   * @param {object} state call-local routes and relocation set
   * @returns {object[]} rows with `retryCoordinator` set
   * @throws {KafkaJSInvariantViolation} for a missing, foreign or duplicate row
   * @throws {KafkaJSShareGroupsNotSupportedError} for a row answered with code 35
   */
  #correlate(answer, groupIds, { routes, relocated }) {
    if (!Array.isArray(answer.groups))
      throw new KafkaJSInvariantViolation('Missing share group results')
    const wanted = new Set(groupIds)
    const seen = new Set()
    const rows = answer.groups.map((row) => {
      if (!wanted.has(row.groupId) || seen.has(row.groupId))
        throw new KafkaJSInvariantViolation('Invalid share group result identity')
      seen.add(row.groupId)
      this.#refused(DESCRIBE_KEY, row.errorCode)
      let retryCoordinator = row.errorCode === COORDINATOR_LOAD_IN_PROGRESS
      if (
        [COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR].includes(row.errorCode) &&
        !relocated.has(row.groupId)
      ) {
        relocated.add(row.groupId)
        routes.delete(row.groupId)
        retryCoordinator = true
      }
      return { ...row, retryCoordinator }
    })
    if (seen.size !== wanted.size) throw new KafkaJSInvariantViolation('Missing share group result')
    return rows
  }

  /**
   * Projects one correlated row into a fresh public description with fresh nested arrays.
   * The signed authorization mask keeps unknown bits; "not requested" and the absent sentinel are
   * `null`. A row that never reached a broker (an unresolved coordinator) keeps only its error.
   * @param {object} row correlated row, or a local-failure row with `errorCode: -1`
   * @param {boolean} includeAuthorizedOperations whether the mask was requested
   * @returns {object} the public group description
   */
  #description(row, includeAuthorizedOperations) {
    return {
      groupId: row.groupId,
      state: row.groupState ?? '',
      groupEpoch: row.groupEpoch ?? 0,
      assignmentEpoch: row.assignmentEpoch ?? 0,
      assignorName: row.assignorName ?? '',
      members: (row.members || []).map((member) => ({
        memberId: member.memberId,
        rackId: member.rackId,
        memberEpoch: member.memberEpoch,
        clientId: member.clientId,
        clientHost: member.clientHost,
        subscribedTopics: member.subscribedTopicNames.slice(),
        assignment: member.assignment.topicPartitions.map((topic) => ({
          topic: topic.topicName,
          topicId: topic.topicId,
          partitions: topic.partitions.slice(),
        })),
      })),
      errorCode: row.errorCode,
      errorMessage: row.errorMessage,
      authorizedOperations:
        !includeAuthorizedOperations ||
        (row.authorizedOperations ?? NO_AUTHORIZED_OPERATIONS) === NO_AUTHORIZED_OPERATIONS
          ? null
          : row.authorizedOperations,
    }
  }

  /**
   * Runs one offset operation on the group's coordinator. Loading (14) is retried in place; a
   * moved coordinator (15/16) is looked up again once; any other group-level code ends the call.
   * @param {string} groupId validated id
   * @param {number} key the API to send
   * @param {string} method Broker method name
   * @param {object} request detached wire fields
   * @returns {Promise<object[]>} the wire topic rows, with runtime 35 already refused
   * @throws {KafkaJSProtocolError} the original group-level error, never a retrier wrapper
   * @throws {KafkaJSShareGroupsNotSupportedError} when the coordinator cannot run the request
   * @throws {KafkaJSInvariantViolation} for an answer that is not about this group
   * @throws {Error} transport failure or the call's deadline
   */
  async #offsets(groupId, key, method, request) {
    let broker
    let relocated = false
    try {
      return await retrierOf(this.retry)(async (bail) => {
        try {
          broker ||= await this.#locate(groupId, key)
          const answer = await this.#send(broker, method, request)
          const group = this.#groupAnswer(answer, groupId, key)
          this.#refused(key, group.errorCode)
          if (group.errorCode !== 0) {
            const error = createErrorFromCode(group.errorCode)
            if (group.errorCode === COORDINATOR_LOAD_IN_PROGRESS) throw error
            if (
              [COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR].includes(group.errorCode) &&
              !relocated
            ) {
              relocated = true
              broker = null
              throw error
            }
            return bail(error)
          }
          const topics = key === LIST_OFFSETS_KEY ? group.topics : group.responses
          for (const topic of topics) {
            if (key === DELETE_OFFSETS_KEY) this.#refused(key, topic.errorCode)
            else for (const partition of topic.partitions) this.#refused(key, partition.errorCode)
          }
          return checked(topics)
        } catch (error) {
          if (
            error instanceof KafkaJSShareGroupsNotSupportedError ||
            error instanceof KafkaJSInvariantViolation
          )
            return bail(error)
          throw error
        }
      })
    } catch (error) {
      currentDeadline()?.check()
      throw causeOf(error, [KafkaJSProtocolError]) || error
    }
  }

  /**
   * Selects the group-level envelope of an offset answer.
   * @param {object} answer decoded response
   * @param {string} groupId the group that was asked about
   * @param {number} key the API that answered
   * @returns {object} the envelope carrying `errorCode`, and `topics` or `responses`
   * @throws {KafkaJSInvariantViolation} when a describe answer is not exactly one row for this group
   */
  #groupAnswer(answer, groupId, key) {
    if (key !== LIST_OFFSETS_KEY) return answer
    if (
      !Array.isArray(answer.groups) ||
      answer.groups.length !== 1 ||
      answer.groups[0].groupId !== groupId
    )
      throw new KafkaJSInvariantViolation('Invalid share offset group result identity')
    return answer.groups[0]
  }
}

/**
 * Creates the share administration family for the admin factory.
 * @param {object} options
 * @param {object} options.cluster supplies GROUP coordinator discovery
 * @param {object} options.retry retry settings of the admin client
 * @returns {ShareGroups} four operations; the factory binds and wraps them
 */
const createShareGroups = (options) => new ShareGroups(options)

module.exports = { createShareGroups }
