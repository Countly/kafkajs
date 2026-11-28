const { MemberAssignment } = require('../consumer/assignerProtocol')
const { createErrorFromCode } = require('../protocol/error')
const { UNCHECKED_ROWS } = require('../broker/privateOptions')
const { withConsumerOperation } = require('./deadline')
const GroupDescriptionResolver = require('./groupDescriptionResolver')
const { WireNames } = require('./wireNames')

/** @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java */
const GROUP_ID_NOT_FOUND = 69
const UNSUPPORTED_VERSION = 35
const COORDINATOR_LOAD_IN_PROGRESS = 14
const COORDINATOR_NOT_AVAILABLE = 15
const NOT_COORDINATOR = 16
/** What the broker sends when authorized operations were not requested. */
const NO_AUTHORIZED_OPERATIONS = -2147483648

/**
 * Group-level codes on a ConsumerGroupDescribe row that mean "ask the older API instead": the group
 * is not a consumer-protocol group (69), or the cluster has the consumer protocol off and answers
 * every row 35, which says nothing about the group.
 * @see https://github.com/apache/kafka/blob/4.4.0/core/src/main/scala/kafka/server/KafkaApis.scala
 */
const ROUTES_TO_CLASSIC = new Set([GROUP_ID_NOT_FOUND, UNSUPPORTED_VERSION])
/** Codes that leave a consumer group unresolved on this API. */
const ASKS_AGAIN = new Set([
  COORDINATOR_LOAD_IN_PROGRESS,
  COORDINATOR_NOT_AVAILABLE,
  NOT_COORDINATOR,
])

/**
 * Whether `error`, or any error it wraps, is the rejection `lookupRequest` raises for an API key
 * the broker does not advertise.
 *
 * The chain is walked rather than the top-level name matched, because the retrier re-wraps what it
 * rejects with (`KafkaJSNonRetriableError` / `KafkaJSNumberOfRetriesExceeded`, the original on
 * `cause`).
 *
 * @param {Error} error
 * @returns {boolean}
 */
const doesNotSupportApiKey = (error) => {
  const seen = new Set()

  for (let current = error; current != null && !seen.has(current); current = current.cause) {
    if (current.name === 'KafkaJSServerDoesNotSupportApiKey') {
      return true
    }

    seen.add(current)
  }

  return false
}

/**
 * How much one row of a ConsumerGroupDescribe answer settles its group id; the highest rank wins.
 * A described group beats an error row that names a code instead of a group, that beats "ask again"
 * (14, 15, 16), that beats "ask the older API" (69, or 35 for a cluster with the consumer protocol
 * off), and a 69 beats a 35 or no answer because only a 69 carries the message to report when both
 * APIs miss. The ranks stay inside one answer to one request: an id settled by an earlier attempt
 * is never ranked against a later one, and a row for an id the request did not ask is never ranked.
 * @param {object} row decoded consumer-group row
 * @returns {number}
 */
const rank = ({ errorCode }) => {
  if (errorCode === 0) return 4
  if (ASKS_AGAIN.has(errorCode)) return 2
  if (!ROUTES_TO_CLASSIC.has(errorCode)) return 3
  return errorCode === GROUP_ID_NOT_FOUND ? 1 : 0
}

/**
 * Describes consumer groups of both protocols behind one shape.
 *
 * Ready ids for the same coordinator and API share a request. The ids whose consumer-protocol row
 * says the group is unknown or that the cluster has that API off (UNSUPPORTED_VERSION), and ids
 * routed to a broker that does not offer key 69, fall back to DescribeGroups (key 15).
 * When both calls report that the group is unknown, the
 * message from the newer API is the one reported, because it says which of the two answers actually
 * applies; after an UNSUPPORTED_VERSION the DescribeGroups row is reported as it is.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/admin/internals/DescribeConsumerGroupsHandler.java
 */
class GroupDescriptions {
  /**
   * @param {object} options
   * @param {import('../cluster')} options.cluster
   * @param {object} options.retry retry configuration for each id on each API
   * @param {import('../../types').Logger} options.logger
   */
  constructor({ cluster, retry, logger }) {
    this.cluster = cluster
    this.retry = retry
    this.logger = logger.namespace('GroupDescriptions')
  }

  /**
   * @param {string[]} groupIds
   * @param {object} [options] original consumer operation and requested authorization flag
   * @returns {Promise<{ groups: object[] }>} one row per id, NOT in input order, each carrying
   *          every field 2.2.4 returned plus the additive ones. An id a coordinator leaves out of
   *          its answer is described through the older API rather than dropped
   * @throws {KafkaJSNonRetriableError} carrying the code, when a lookup or request fails in a way
   *         that is not retried: for an id that fell back to DescribeGroups, any group-level code
   *         that API reports but GROUP_ID_NOT_FOUND (v6), 14, 15 and 16 (an authorization failure,
   *         say). Such a failure ends the call as soon as it arrives. A group-level error the newer
   *         API reports stays on that group's row
   * @throws {KafkaJSNumberOfRetriesExceeded} when ids are still unresolved once the retry budget
   *         for an id on its current API is spent; the cause is its last attempt's failure
   *
   *         Each written id (a lone surrogate is written as U+FFFD, so the ids a caller gives can
   *         be written alike) is sent once per attempt and settles one outcome from that request.
   *         Of several rows for one id, a described group beats an error row, an error row beats a
   *         14, 15 or 16, that beats a 69, and that beats a 35. The first of equal rank wins.
   *         Each retry asks only for unresolved ids. A move refreshes metadata and discovers a
   *         new route. An outcome from the consumer protocol gets one row per caller occurrence.
   *         A classic outcome gets one row per distinct caller spelling. Every additional
   *         projected row is an independent copy under the exact caller spelling.
   */
  async describe(groupIds, options = {}) {
    const { operation } = options
    return withConsumerOperation(operation, () =>
      operation
        ? operation.wait(() => this.#describe(groupIds, options))
        : this.#describe(groupIds, options)
    )
  }

  /**
   * Resolves wire ids and projects the chosen rows under the original caller spellings.
   * @param {string[]} groupIds requested group ids
   * @param {object} options original consumer operation and authorization flag
   * @returns {Promise<{groups: object[]}>} the unified descriptions
   */
  async #describe(groupIds, { operation, includeAuthorizedOperations = false }) {
    if (typeof includeAuthorizedOperations !== 'boolean') {
      const { KafkaJSInvalidConfigurationError } = require('../errors')
      throw new KafkaJSInvalidConfigurationError('includeAuthorizedOperations must be a boolean')
    }

    const names = new WireNames(groupIds)
    const request = {
      includeAuthorizedOperations,
      ...(operation && { guard: operation.guard }),
    }
    const resolver = new GroupDescriptionResolver({
      cluster: this.cluster,
      retry: this.retry,
      operation,
      consumer: {
        ask: async (broker, groupIds) => {
          try {
            return (await broker.consumerGroupDescribe({ ...request, groupIds })).groups
          } catch (error) {
            if (!doesNotSupportApiKey(error)) throw error
            return []
          }
        },
        outcomes: (rows, ids) => this.#consumerOutcomes(rows, ids),
      },
      classic: {
        ask: async (broker, groupIds) => {
          const { groups } = await broker.describeGroups(
            { ...request, groupIds },
            { [UNCHECKED_ROWS]: true }
          )
          return groups
        },
        outcomes: (rows, ids) => this.#classicOutcomes(rows, ids),
      },
    })
    const { consumer, classic } = await resolver.resolve(names.ids)
    const groups = []
    for (const row of consumer) {
      names.project(
        groups,
        row.groupId,
        this.#shapeConsumerGroup(row, includeAuthorizedOperations),
        true
      )
    }
    for (const { row, missing } of classic) {
      names.project(
        groups,
        row.groupId,
        this.#shapeClassicGroup(row, missing, includeAuthorizedOperations),
        false
      )
    }
    return { groups }
  }

  /**
   * Selects one raw row per requested id. Kafka 4.0 can return a group from its owning shard and
   * additional rows for the same id from other shards. The first row of equal rank wins.
   * @param {object[]} rows decoded ConsumerGroupDescribe rows
   * @param {Set<string>} ids requested wire ids
   * @returns {Map<string, object>} success, fallback or retriable error per requested id
   */
  #consumerOutcomes(rows, ids) {
    const selected = new Map()
    for (const row of rows) {
      if (!ids.has(row.groupId)) continue
      const previous = selected.get(row.groupId)
      if (!previous || rank(row) > rank(previous)) selected.set(row.groupId, row)
    }

    const outcomes = new Map()
    for (const [id, row] of selected) {
      const { errorCode, errorMessage } = row
      if (ASKS_AGAIN.has(errorCode)) {
        outcomes.set(id, { error: createErrorFromCode(errorCode) })
      } else if (ROUTES_TO_CLASSIC.has(errorCode)) {
        outcomes.set(id, {
          fallback: true,
          missing: errorCode === GROUP_ID_NOT_FOUND ? { errorCode, errorMessage } : undefined,
        })
      } else {
        outcomes.set(id, { row })
      }
    }
    for (const id of ids) {
      if (!outcomes.has(id)) outcomes.set(id, { fallback: true })
    }
    return outcomes
  }

  /**
   * Keeps the first classic outcome per id. The negotiated response reader has already checked
   * membership and attached errors for the rows the version's whole-response parser would reject.
   * @param {object[]} rows decoded DescribeGroups rows
   * @param {Set<string>} ids requested wire ids
   * @returns {Map<string, object>} first result or error per answered id
   */
  #classicOutcomes(rows, ids) {
    const outcomes = new Map()
    for (const row of rows) {
      if (ids.has(row.groupId) && !outcomes.has(row.groupId)) {
        outcomes.set(row.groupId, row.error === undefined ? { row } : { error: row.error })
      }
    }
    return outcomes
  }

  /**
   * A ConsumerGroupDescribe row in the unified shape.
   * @private
   * @param {object} row
   * @param {boolean} requested whether the mask was requested
   * @returns {object}
   */
  #shapeConsumerGroup(row, requested) {
    // An errored row describes nothing: the coordinator answered a code instead of a group, so its
    // type and protocol are unknown rather than 'consumer'. Reporting them as 'consumer' would make
    // a dashboard call a classic group the caller merely lacks a Describe ACL for a KIP-848 group.
    const described = row.errorCode === 0

    return {
      groupId: row.groupId,
      // ConsumerGroupDescribe carries no protocolType. It is produced here so that tooling written
      // against a classic description keeps working against a consumer group.
      protocolType: described ? 'consumer' : '',
      // What kafka-consumer-groups.sh shows in the PROTOCOL column for such a group.
      protocol: row.assignorName || '',
      state: row.groupState,
      errorCode: row.errorCode,
      errorMessage: row.errorMessage == null ? null : row.errorMessage,
      groupType: described ? 'consumer' : '',
      groupEpoch: row.groupEpoch,
      assignmentEpoch: row.assignmentEpoch,
      assignorName: row.assignorName == null ? null : row.assignorName,
      authorizedOperations:
        !requested ||
        row.authorizedOperations == null ||
        row.authorizedOperations === NO_AUTHORIZED_OPERATIONS
          ? null
          : row.authorizedOperations,
      members: (row.members || []).map((member) => this.#shapeMember(member, 'consumer')),
    }
  }

  /**
   * A DescribeGroups row in the same shape, carrying the stashed key-69 error when both calls
   * reported the group unknown.
   *
   * Such a row carries `errorCode` 0 or GROUP_ID_NOT_FOUND (69) and nothing else: a row with any
   * other code carries the error DescribeGroups' own rule gives it, which is asked again or ends
   * the call instead of being shaped.
   * @private
   * @param {object} row
   * @param {{errorCode: number, errorMessage: string|null}} [stashed]
   * @param {boolean} requested whether the mask was requested
   * @returns {object}
   */
  #shapeClassicGroup(row, stashed, requested) {
    // The Java admin rejects a classic group whose protocolType is neither "consumer" nor empty.
    // kafkajs has always returned such groups, so this client keeps returning them.
    // @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/admin/internals/DescribeConsumerGroupsHandler.java
    const bothMissed = stashed != null && row.errorCode === GROUP_ID_NOT_FOUND

    return {
      groupId: row.groupId,
      protocolType: row.protocolType,
      protocol: row.protocol,
      state: row.state,
      errorCode: bothMissed ? stashed.errorCode : row.errorCode,
      errorMessage: bothMissed
        ? stashed.errorMessage
        : row.errorMessage == null
          ? null
          : row.errorMessage,
      groupType: 'classic',
      groupEpoch: null,
      assignmentEpoch: null,
      assignorName: null,
      authorizedOperations:
        !requested ||
        row.authorizedOperations == null ||
        row.authorizedOperations === NO_AUTHORIZED_OPERATIONS
          ? null
          : row.authorizedOperations,
      members: (row.members || []).map((member) =>
        this.#shapeMember(member, 'classic', row.protocolType)
      ),
    }
  }

  /**
   * One `MemberDescription`.
   *
   * For a consumer-group member both buffers are synthesised: ConsumerGroupDescribe carries neither,
   * and producing them here is what keeps tooling that decodes a classic member's buffers working.
   * @private
   * @param {object} member
   * @param {'classic'|'consumer'} kind
   * @param {string} [protocolType] the classic group's protocol type, which decides whether its
   *   members' assignment buffers are decodable at all
   * @returns {object}
   */
  #shapeMember(member, kind, protocolType) {
    if (kind === 'consumer') {
      const assignment = (member.assignment ? member.assignment.topicPartitions || [] : []).map(
        ({ topicName, partitions }) => ({ topic: topicName, partitions })
      )
      const target = member.targetAssignment
        ? (member.targetAssignment.topicPartitions || []).map(({ topicName, partitions }) => ({
            topic: topicName,
            partitions,
          }))
        : null

      return {
        memberId: member.memberId,
        clientId: member.clientId,
        clientHost: member.clientHost,
        memberMetadata: Buffer.alloc(0),
        memberAssignment: MemberAssignment.encode({
          version: 0,
          assignment: assignment.reduce(
            (byTopic, { topic, partitions }) => ({ ...byTopic, [topic]: partitions }),
            {}
          ),
        }),
        groupInstanceId: member.instanceId == null ? null : member.instanceId,
        rackId: member.rackId == null ? null : member.rackId,
        memberEpoch: member.memberEpoch,
        assignment,
        targetAssignment: target,
        memberType:
          member.memberType === 1 ? 'consumer' : member.memberType === 0 ? 'classic' : null,
        subscribedTopics: member.subscribedTopicNames || [],
        subscribedTopicRegex:
          member.subscribedTopicRegex == null ? null : member.subscribedTopicRegex,
      }
    }

    return {
      memberId: member.memberId,
      clientId: member.clientId,
      clientHost: member.clientHost,
      memberMetadata: member.memberMetadata,
      memberAssignment: member.memberAssignment,
      groupInstanceId: member.groupInstanceId == null ? null : member.groupInstanceId,
      rackId: null,
      memberEpoch: null,
      assignment: this.#decodeClassicAssignment(member.memberAssignment, protocolType),
      targetAssignment: null,
      memberType: null,
      subscribedTopics: [],
      subscribedTopicRegex: null,
    }
  }

  /**
   * A classic member's assignment, decoded only when the group's protocol type says the buffer is
   * this client's own format. Anything else is another framework's payload and is left alone.
   * @private
   * @param {Buffer} buffer
   * @param {string} protocolType
   * @returns {Array<{topic: string, partitions: number[]}>}
   */
  #decodeClassicAssignment(buffer, protocolType) {
    if (protocolType !== 'consumer' || !buffer || buffer.length === 0) {
      return []
    }

    try {
      const { assignment } = MemberAssignment.decode(buffer)
      return Object.entries(assignment).map(([topic, partitions]) => ({ topic, partitions }))
    } catch (e) {
      this.logger.debug('Could not decode a member assignment buffer', { error: e.message })
      return []
    }
  }
}

/**
 * @param {{ cluster: import('../cluster'), retry: object, logger: import('../../types').Logger }} options
 * @returns {GroupDescriptions}
 */
const createGroupDescriptions = (options) => new GroupDescriptions(options)

module.exports = { createGroupDescriptions }
