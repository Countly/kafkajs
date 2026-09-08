const { MemberAssignment } = require('../consumer/assignerProtocol')
const { createErrorFromCode } = require('../protocol/error')
const createRetry = require('../retry')

/** @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java */
const GROUP_ID_NOT_FOUND = 69
const UNSUPPORTED_VERSION = 35
const COORDINATOR_LOAD_IN_PROGRESS = 14
const COORDINATOR_NOT_AVAILABLE = 15
const NOT_COORDINATOR = 16
/** What the broker sends when authorized operations were not requested. */
const NO_AUTHORIZED_OPERATIONS = -2147483648

/** Group-level codes on a ConsumerGroupDescribe row that mean "ask the older API instead". */
const ROUTES_TO_CLASSIC = new Set([GROUP_ID_NOT_FOUND, UNSUPPORTED_VERSION])
/** Codes that mean the coordinator moved: find it again before the next attempt. */
const RE_FINDS_COORDINATOR = new Set([COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR])
/** Codes that make the whole batch worth re-sending, on the retry budget's backoff. */
const RETRIES_BATCH = new Set([COORDINATOR_LOAD_IN_PROGRESS, ...RE_FINDS_COORDINATOR])

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
 * Describes consumer groups of both protocols behind one shape.
 *
 * Routing is PER COORDINATOR, as it has always been: the ids are grouped by their coordinator and
 * each coordinator is asked once for a ConsumerGroupDescribe (key 69). Only the fallback decision is
 * per id - the ids whose row says the group is unknown to the new API, or that the broker does not
 * offer key 69 at all, are re-asked in one DescribeGroups (key 15) per coordinator. When both calls
 * report that the group is unknown, the message from the newer API is the one reported, because it
 * says which of the two answers actually applies.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/admin/internals/DescribeConsumerGroupsHandler.java
 */
class GroupDescriptions {
  /**
   * @param {object} options
   * @param {import('../cluster')} options.cluster
   * @param {object} options.retry retry configuration for the per-coordinator calls
   * @param {import('../../types').Logger} options.logger
   */
  constructor({ cluster, retry, logger }) {
    this.cluster = cluster
    this.retry = retry
    this.logger = logger.namespace('GroupDescriptions')
  }

  /**
   * @param {string[]} groupIds
   * @returns {Promise<{ groups: object[] }>} one row per id, NOT in input order, each carrying
   *          every field 2.2.4 returned plus the additive ones. An id a coordinator leaves out of
   *          its answer is described through the older API rather than dropped
   * @throws {KafkaJSNumberOfRetriesExceeded} when a coordinator never settles within the retry
   *         budget, and whatever a transport or a protocol error raises - a group-level error the
   *         newer API reports stays on that group's row instead
   */
  async describe(groupIds) {
    const described = await this.#describeByCoordinator(groupIds)
    const rows = described.filter(({ needsFallback }) => !needsFallback).map(({ row }) => row)
    const fallbacks = described.filter(({ needsFallback }) => needsFallback)

    if (fallbacks.length === 0) {
      return { groups: rows }
    }

    const classic = await this.#describeClassicByCoordinator(
      fallbacks.map(({ groupId }) => groupId)
    )
    const stashed = new Map(fallbacks.map(({ groupId, error }) => [groupId, error]))

    return {
      groups: [
        ...rows,
        ...classic.map((row) => this.#shapeClassicGroup(row, stashed.get(row.groupId))),
      ],
    }
  }

  /**
   * One ConsumerGroupDescribe per coordinator, with each id's row either shaped or marked for the
   * classic fallback. A coordinator that does not implement key 69 sends every one of its ids to
   * the fallback in one go.
   * @private
   * @param {string[]} groupIds
   * @returns {Promise<Array<{ groupId: string, row?: object, needsFallback: boolean, error?: {errorCode: number, errorMessage: string|null} }>>}
   */
  async #describeByCoordinator(groupIds) {
    const batches = await this.#groupByCoordinator(groupIds)
    const perBatch = await Promise.all(batches.map((batch) => this.#describeBatch(batch)))

    return perBatch.flat()
  }

  /**
   * One coordinator's ConsumerGroupDescribe, retried on the coordinator's own codes.
   *
   * `parse` returns group-level codes rather than throwing them, so a coordinator that is still
   * loading (14) or has moved (15, 16) is turned into a throw here and the batch is re-sent by the
   * configured retrier - the budget and exponential backoff this call has always run under. A
   * moved coordinator is looked up again before the next attempt; a loading one is asked again
   * where it is. When the budget runs out the retrier's error is what the caller sees.
   *
   * @private
   * @param {{ coordinator: object, ids: string[] }} batch
   * @returns {Promise<Array<{ groupId: string, row?: object, needsFallback: boolean, error?: object }>>}
   * @throws {KafkaJSNumberOfRetriesExceeded} when the coordinator never settles within the budget
   */
  async #describeBatch({ coordinator, ids }) {
    let target = coordinator
    let coordinatorMoved = false
    let response

    try {
      response = await createRetry(this.retry)(async () => {
        if (coordinatorMoved) {
          await this.cluster.refreshMetadata()
          target = await this.cluster.findGroupCoordinator({ groupId: ids[0] })
          coordinatorMoved = false
        }

        const answer = await target.consumerGroupDescribe({ groupIds: ids })
        const unsettled = answer.groups.find(({ errorCode }) => RETRIES_BATCH.has(errorCode))

        if (unsettled) {
          coordinatorMoved = RE_FINDS_COORDINATOR.has(unsettled.errorCode)
          throw createErrorFromCode(unsettled.errorCode)
        }

        return answer
      })
    } catch (e) {
      if (doesNotSupportApiKey(e)) {
        this.logger.debug('Broker does not offer ConsumerGroupDescribe, describing as classic', {
          nodeId: coordinator.nodeId,
          groupIds: ids,
        })

        return ids.map((groupId) => ({ groupId, needsFallback: true }))
      }

      throw e
    }

    const answered = new Set(response.groups.map(({ groupId }) => groupId))

    return [
      ...response.groups.map((row) => this.#routeRow(row)),
      // An id the coordinator left out of its answer is described through the older API instead,
      // with no error to carry forward, rather than being dropped from the result.
      ...ids
        .filter((groupId) => !answered.has(groupId))
        .map((groupId) => ({ groupId, needsFallback: true })),
    ]
  }

  /**
   * What to do with one ConsumerGroupDescribe row: shape it, or route it to the older API.
   * @private
   * @param {object} row
   * @returns {{ groupId: string, row?: object, needsFallback: boolean, error?: object }}
   */
  #routeRow(row) {
    const { groupId, errorCode, errorMessage } = row

    if (ROUTES_TO_CLASSIC.has(errorCode)) {
      // Not an error the caller sees: it is the signal that this group belongs to the older API.
      // The message is stashed, because a double miss reports THIS one.
      return { groupId, needsFallback: true, error: { errorCode, errorMessage } }
    }

    // Anything else - including an authorization failure - surfaces on the row rather than
    // throwing, so one bad id cannot fail the whole call.
    return { groupId, needsFallback: false, row: this.#shapeConsumerGroup(row) }
  }

  /**
   * One DescribeGroups per coordinator for the ids that routed.
   * @private
   * @param {string[]} groupIds
   * @returns {Promise<object[]>} the raw key-15 rows
   */
  async #describeClassicByCoordinator(groupIds) {
    const batches = await this.#groupByCoordinator(groupIds)

    const perBatch = await Promise.all(
      batches.map(async ({ coordinator, ids }) => {
        const { groups } = await createRetry(this.retry)(() =>
          coordinator.describeGroups({ groupIds: ids })
        )

        return groups
      })
    )

    return perBatch.flat()
  }

  /**
   * FindCoordinator for every id, then one batch per distinct coordinator node - the shape this
   * client has always used, kept so N ids do not become N round trips.
   * @private
   * @param {string[]} groupIds
   * @returns {Promise<Array<{ coordinator: object, ids: string[] }>>}
   */
  async #groupByCoordinator(groupIds) {
    const located = await Promise.all(
      groupIds.map(async (groupId) => ({
        groupId,
        coordinator: await this.cluster.findGroupCoordinator({ groupId }),
      }))
    )

    const batches = new Map()
    for (const { groupId, coordinator } of located) {
      const batch = batches.get(coordinator.nodeId) || { coordinator, ids: [] }
      batch.ids.push(groupId)
      batches.set(coordinator.nodeId, batch)
    }

    return [...batches.values()]
  }

  /**
   * A ConsumerGroupDescribe row in the unified shape.
   * @private
   * @param {object} row
   * @returns {object}
   */
  #shapeConsumerGroup(row) {
    return {
      groupId: row.groupId,
      // ConsumerGroupDescribe carries no protocolType. It is produced here so that tooling written
      // against a classic description keeps working against a consumer group.
      protocolType: 'consumer',
      // What kafka-consumer-groups.sh shows in the PROTOCOL column for such a group.
      protocol: row.assignorName || '',
      state: row.groupState,
      errorCode: row.errorCode,
      errorMessage: row.errorMessage == null ? null : row.errorMessage,
      groupType: 'consumer',
      groupEpoch: row.groupEpoch,
      assignmentEpoch: row.assignmentEpoch,
      assignorName: row.assignorName == null ? null : row.assignorName,
      authorizedOperations:
        row.authorizedOperations === NO_AUTHORIZED_OPERATIONS ? null : row.authorizedOperations,
      members: (row.members || []).map((member) => this.#shapeMember(member, 'consumer')),
    }
  }

  /**
   * A DescribeGroups row in the same shape, carrying the stashed key-69 error when both calls
   * reported the group unknown.
   * @private
   * @param {object} row
   * @param {{errorCode: number, errorMessage: string|null}} [stashed]
   * @returns {object}
   */
  #shapeClassicGroup(row, stashed) {
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
        row.authorizedOperations === NO_AUTHORIZED_OPERATIONS ? null : row.authorizedOperations,
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
