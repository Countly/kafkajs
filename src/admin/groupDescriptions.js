const { MemberAssignment } = require('../consumer/assignerProtocol')
const { createErrorFromCode } = require('../protocol/error')
const { retrierOf, currentDeadline, withConsumerOperation } = require('./deadline')

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
   * Preserves the consumer's operation retry executor; ordinary admin calls use the running deadline.
   * @param {object} context per-call immutable operation reference
   * @returns {Function} existing counted retrier under the captured operation
   */
  #retrier(context) {
    const operation = context.operation
    if (operation?.retry) return (action) => operation.retry(action, this.retry)
    return operation?.retrier ? operation.retrier(this.retry) : retrierOf(this.retry)
  }

  /**
   * Checks the captured consumer/admin operation in its awaiting continuation.
   * @param {object} context original invocation
   * @param {any} value result to publish only while current
   * @returns {any} unchanged result
   * @throws {Error} original operation refusal
   */
  #checked(context, value) {
    context.operation?.check()
    return value
  }

  /**
   * Lazily enters a captured operation's wait, preserving consumer outside admin context.
   * @param {object} context original invocation
   * @param {Function} work not-yet-started operation
   * @returns {Promise<any>|any} original result or bounded local wait
   */
  #wait(context, work) {
    return context.operation ? context.operation.wait(work) : work()
  }

  /**
   * @param {string[]} groupIds
   * @param {object} [options] original consumer operation and requested authorization flag
   * @returns {Promise<{ groups: object[] }>} one row per id, NOT in input order, each carrying
   *          every field 2.2.4 returned plus the additive ones. An id a coordinator leaves out of
   *          its answer is described through the older API rather than dropped
   * @throws {KafkaJSNumberOfRetriesExceeded} when a coordinator never settles within the retry
   *         budget, and whatever a transport or a protocol error raises. A group-level error the
   *         newer API reports stays on that group's row instead; one DescribeGroups reports for an
   *         id that fell back to it fails the call, because that API's own parse throws every
   *         group-level code but GROUP_ID_NOT_FOUND
   */
  async describe(groupIds, options = {}) {
    return withConsumerOperation(options.operation, async () => {
      const coordinators = new Map()
      const operation = currentDeadline()
      const unown = operation?.own(() => coordinators.clear())
      try {
        return await this.#describe(groupIds, { ...options, coordinators })
      } finally {
        unown?.()
        coordinators.clear()
      }
    })
  }

  /**
   * Describes both protocols within the original operation and a call-local coordinator map.
   * @param {string[]} groupIds requested ids
   * @param {object} options original consumer operation and optional authorization flag
   * @returns {Promise<{groups: object[]}>} unified rows
   * @throws {Error} original protocol, retry, consumer or admin refusal
   */
  async #describe(groupIds, { operation, coordinators, includeAuthorizedOperations = false } = {}) {
    if (typeof includeAuthorizedOperations !== 'boolean') {
      const { KafkaJSInvalidConfigurationError } = require('../errors')
      throw new KafkaJSInvalidConfigurationError('includeAuthorizedOperations must be a boolean')
    }
    const context = {
      operation: operation || currentDeadline(),
      consumerOperation: operation,
      includeAuthorizedOperations,
      coordinators,
    }
    const described = this.#checked(
      context,
      await this.#wait(context, async () => this.#describeByCoordinator(groupIds, context))
    )
    const rows = described.filter(({ needsFallback }) => !needsFallback).map(({ row }) => row)
    const fallbacks = described.filter(({ needsFallback }) => needsFallback)

    if (fallbacks.length === 0) {
      return { groups: rows }
    }

    const classic = this.#checked(
      context,
      await this.#wait(context, async () =>
        this.#describeClassicByCoordinator(
          fallbacks.map(({ groupId }) => groupId),
          context
        )
      )
    )
    const stashed = new Map(fallbacks.map(({ groupId, error }) => [groupId, error]))

    return {
      groups: [
        ...rows,
        ...classic.map((row) =>
          this.#shapeClassicGroup(
            row,
            stashed.get(row.groupId),
            context.includeAuthorizedOperations
          )
        ),
      ],
    }
  }

  /**
   * One ConsumerGroupDescribe per coordinator, with each id's row either shaped or marked for the
   * classic fallback. A coordinator that does not implement key 69 sends every one of its ids to
   * the fallback in one go.
   * @private
   * @param {string[]} groupIds
   * @param {object} context captured operation and per-call coordinator map
   * @returns {Promise<Array<{ groupId: string, row?: object, needsFallback: boolean, error?: {errorCode: number, errorMessage: string|null} }>>}
   */
  async #describeByCoordinator(groupIds, context) {
    const batches = this.#checked(
      context,
      await this.#wait(context, async () => this.#groupByCoordinator(groupIds, context))
    )
    const perBatch = this.#checked(
      context,
      await this.#wait(context, async () =>
        Promise.all(batches.map((batch) => this.#describeBatch(batch, context)))
      )
    )

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
   * @param {object} context captured operation and per-call coordinator map
   * @returns {Promise<Array<{ groupId: string, row?: object, needsFallback: boolean, error?: object }>>}
   * @throws {KafkaJSNumberOfRetriesExceeded} when the coordinator never settles within the budget
   */
  async #describeBatch({ coordinator, ids }, context) {
    let target = coordinator
    let batchIds = ids
    let coordinatorMoved = false
    let response

    try {
      response = this.#checked(
        context,
        await this.#wait(context, async () =>
          this.#retrier(context)(async () => {
            if (coordinatorMoved) {
              this.#checked(
                context,
                await this.#wait(context, async () =>
                  this.cluster.refreshMetadata(
                    ...(context.consumerOperation ? [{ fence: context.consumerOperation }] : [])
                  )
                )
              )
              // Regrouped, not re-pointed at the first id's new coordinator: a `__consumer_offsets`
              // leader move can split ids that shared a coordinator across two brokers, and sending
              // the whole batch to one of them would keep drawing NOT_COORDINATOR for the rest until
              // the budget ran out. Each sub-batch retries on its own from here.
              for (const id of batchIds) context.coordinators.delete(id)
              const regrouped = this.#checked(
                context,
                await this.#wait(context, async () => this.#groupByCoordinator(batchIds, context))
              )

              if (regrouped.length > 1) {
                const perBatch = this.#checked(
                  context,
                  await this.#wait(context, async () =>
                    Promise.all(regrouped.map((batch) => this.#describeBatch(batch, context)))
                  )
                )
                return { split: perBatch.flat() }
              }

              target = regrouped[0].coordinator
              batchIds = regrouped[0].ids
              coordinatorMoved = false
            }

            const answer = this.#checked(
              context,
              await this.#wait(context, async () =>
                target.consumerGroupDescribe({
                  groupIds: batchIds,
                  ...(context.consumerOperation && { guard: context.consumerOperation.guard }),
                  includeAuthorizedOperations: context.includeAuthorizedOperations,
                })
              )
            )
            const unsettled = answer.groups.find(({ errorCode }) => RETRIES_BATCH.has(errorCode))

            if (unsettled) {
              coordinatorMoved = RE_FINDS_COORDINATOR.has(unsettled.errorCode)
              throw createErrorFromCode(unsettled.errorCode)
            }

            return answer
          })
        )
      )
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

    // The batch split across coordinators: the sub-batches above already produced the routed rows.
    if (response.split) {
      return response.split
    }

    const answered = new Set(response.groups.map(({ groupId }) => groupId))

    return [
      ...response.groups.map((row) => this.#routeRow(row, context)),
      // An id the coordinator left out of its answer is described through the older API instead,
      // with no error to carry forward, rather than being dropped from the result.
      ...batchIds
        .filter((groupId) => !answered.has(groupId))
        .map((groupId) => ({ groupId, needsFallback: true })),
    ]
  }

  /**
   * What to do with one ConsumerGroupDescribe row: shape it, or route it to the older API.
   * @private
   * @param {object} row
   * @param {object} context captured operation and requested authorization flag
   * @returns {{ groupId: string, row?: object, needsFallback: boolean, error?: object }}
   */
  #routeRow(row, context) {
    const { groupId, errorCode, errorMessage } = row

    if (ROUTES_TO_CLASSIC.has(errorCode)) {
      // Not an error the caller sees: it is the signal that this group belongs to the older API.
      // The message is stashed, because a double miss reports THIS one.
      return { groupId, needsFallback: true, error: { errorCode, errorMessage } }
    }

    // Anything else - including an authorization failure - surfaces on the row rather than
    // throwing, so one bad id cannot fail the whole call.
    return {
      groupId,
      needsFallback: false,
      row: this.#shapeConsumerGroup(row, context.includeAuthorizedOperations),
    }
  }

  /**
   * One DescribeGroups per coordinator for the ids that routed.
   * @private
   * @param {string[]} groupIds
   * @param {object} context captured operation and per-call coordinator map
   * @returns {Promise<object[]>} the raw key-15 rows
   */
  async #describeClassicByCoordinator(groupIds, context) {
    const batches = this.#checked(
      context,
      await this.#wait(context, async () => this.#groupByCoordinator(groupIds, context))
    )
    return this.#checked(
      context,
      await this.#wait(context, async () =>
        Promise.all(batches.map((batch) => this.#describeClassicBatch(batch.ids, context)))
      )
    ).flat()
  }

  /**
   * Retries only unresolved classic ids using their located coordinators; a move can split a batch.
   * @param {string[]} ids original ids on one initial coordinator
   * @param {object} context original consumer/admin operation and per-call map
   * @returns {Promise<object[]>} raw classic rows
   * @throws {Error} original retry exhaustion, response error, or operation refusal
   */
  async #describeClassicBatch(ids, context) {
    const pending = new Set(ids)
    const completed = new Map()
    this.#checked(
      context,
      await this.#wait(context, async () =>
        this.#retrier(context)(async () => {
          const batches = this.#checked(
            context,
            await this.#wait(context, async () => this.#groupByCoordinator([...pending], context))
          )
          this.#checked(
            context,
            await this.#wait(context, async () =>
              Promise.all(
                batches.map(async ({ coordinator, ids: batchIds }) => {
                  try {
                    const { groups } = this.#checked(
                      context,
                      await this.#wait(context, async () =>
                        coordinator.describeGroups({
                          groupIds: batchIds,
                          ...(context.consumerOperation && {
                            guard: context.consumerOperation.guard,
                          }),
                          includeAuthorizedOperations: context.includeAuthorizedOperations,
                        })
                      )
                    )
                    for (const row of groups) {
                      completed.set(row.groupId, row)
                      pending.delete(row.groupId)
                    }
                  } catch (error) {
                    if (RE_FINDS_COORDINATOR.has(error.code)) {
                      for (const id of batchIds) context.coordinators.delete(id)
                    }
                    throw error
                  }
                })
              )
            )
          )
        })
      )
    )
    return [...completed.values()]
  }

  /**
   * FindCoordinator for every id, then one batch per distinct coordinator node - the shape this
   * client has always used, kept so N ids do not become N round trips.
   * @private
   * @param {string[]} groupIds
   * @param {object} context captured operation and per-call coordinator map
   * @returns {Promise<Array<{ coordinator: object, ids: string[] }>>}
   */
  async #groupByCoordinator(groupIds, context) {
    const located = this.#checked(
      context,
      await this.#wait(context, async () =>
        Promise.all(
          groupIds.map(async (groupId) => {
            let coordinator = context.coordinators.get(groupId)
            if (!coordinator) {
              coordinator = this.#checked(
                context,
                await this.#wait(context, async () =>
                  this.cluster.findGroupCoordinator({
                    groupId,
                    ...(context.consumerOperation && { fence: context.consumerOperation }),
                    guard: currentDeadline()?.guard,
                  })
                )
              )
              context.coordinators.set(groupId, coordinator)
            }
            return { groupId, coordinator }
          })
        )
      )
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
   * Such a row carries `errorCode` 0 or GROUP_ID_NOT_FOUND (69) and nothing else: DescribeGroups'
   * own `parse` throws every other group-level code, so the call fails before a row exists.
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
