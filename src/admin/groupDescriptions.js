const { MemberAssignment } = require('../consumer/assignerProtocol')
const { createErrorFromCode } = require('../protocol/error')
const { retrierOf, currentDeadline, withConsumerOperation } = require('./deadline')
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
 * How much one row of a ConsumerGroupDescribe answer settles its group id; the highest rank wins.
 * A described group beats an error row that names a code instead of a group, that beats "ask again"
 * (14, 15, 16), that beats "ask the older API" (69, or 35 for a cluster with the consumer protocol
 * off), and a 69 beats a 35 or no answer because only a 69 carries the message to report when both
 * APIs miss. The ranks stay inside one answer: an id settled by an earlier attempt is never
 * ranked against a later one.
 * @param {{ retry?: number, needsFallback: boolean, row?: object, error?: object }} entry
 * @returns {number}
 */
const rank = ({ retry, needsFallback, row, error }) => {
  if (retry !== undefined) return 2
  if (needsFallback) return error ? 1 : 0
  return row.errorCode === 0 ? 4 : 3
}

/**
 * Settles ONE entry per group id from the rows of one answer. A broker can answer an id more than
 * once in one reply, and not consistently: Kafka 4.0.0 hands every offsets shard the whole id list
 * (`GroupCoordinatorService.consumerGroupDescribe` passes `groupIds`, not the shard's `groupList`),
 * so the shard that owns a group answers it described while the others answer it unknown (69),
 * and a shard that cannot serve its own ids answers 14, 15 or 16 for those only; 4.4.0 hands each
 * shard its own ids. A row that outranks the others (see `rank`) is the answer; of equal rank the
 * first is, as Java's handler keys its results by group id. The map keeps ids in the order they
 * first appear in the answer.
 * @param {Array<{ groupId: string, retry?: number, needsFallback: boolean, row?: object, error?: object }>} entries
 * @returns {Map<string, { groupId: string, retry?: number, needsFallback: boolean, row?: object, error?: object }>}
 */
const settle = (entries) => {
  const settled = new Map()
  for (const entry of entries) {
    const held = settled.get(entry.groupId)
    if (held === undefined || rank(entry) > rank(held)) settled.set(entry.groupId, entry)
  }
  return settled
}

/**
 * Describes consumer groups of both protocols behind one shape.
 *
 * Routing is PER COORDINATOR, as it has always been: the ids are grouped by their coordinator and
 * each coordinator is asked once for a ConsumerGroupDescribe (key 69). Only the fallback decision is
 * per id - the ids whose row says the group is unknown to the new API or that the cluster has that
 * API off (UNSUPPORTED_VERSION), or that the broker does not offer key 69 at all, are re-asked in one
 * DescribeGroups (key 15) per coordinator. When both calls report that the group is unknown, the
 * message from the newer API is the one reported, because it says which of the two answers actually
 * applies; after an UNSUPPORTED_VERSION the DescribeGroups row is reported as it is.
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
   *
   *         Each written id (a lone surrogate is written as U+FFFD, so the ids a caller gives can
   *         be written alike) is sent, routed and retried once, whatever spellings and repeats were
   *         given, and settles ONE outcome from one answer: of several rows for it, a described
   *         group beats an error row, an error row beats a 14, 15 or 16, that beats a 69 or 35, and
   *         the first of equal rank wins. An id that was answered 14, 15 or 16, or whose request
   *         threw, is the only one asked again; ids already settled keep their answer, and only the
   *         ids that moved are looked up again. The outcome is then given to the caller's
   *         spellings under each exact spelling, a well-formed one never renamed. An outcome from
   *         the consumer protocol gets one row per occurrence of an id the caller gave (a repeated
   *         id has two rows); one from the classic API gets one row per distinct spelling. Each row
   *         beyond the first for an id is an independent copy: changing one never changes another
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
    const names = new WireNames(groupIds)
    const described = this.#checked(
      context,
      await this.#wait(context, async () =>
        this.#resolve(names.ids, context, (coordinator, ids) =>
          this.#askConsumer(coordinator, ids, context)
        )
      )
    )
    const groups = []
    const fallbacks = []
    for (const entry of described) {
      if (entry.needsFallback) fallbacks.push(entry)
      else names.project(groups, entry.groupId, entry.row, true)
    }

    if (fallbacks.length === 0) {
      return { groups }
    }

    const classic = this.#checked(
      context,
      await this.#wait(context, async () =>
        this.#resolve(
          fallbacks.map(({ groupId }) => groupId),
          context,
          (coordinator, ids) => this.#askClassic(coordinator, ids, context)
        )
      )
    )
    const stashed = new Map(fallbacks.map(({ groupId, error }) => [groupId, error]))

    for (const row of classic) {
      const shaped = this.#shapeClassicGroup(
        row,
        stashed.get(row.groupId),
        context.includeAuthorizedOperations
      )
      names.project(groups, row.groupId, shaped, false)
    }
    return { groups }
  }

  /**
   * Resolves every id through `ask`, one request per coordinator, and asks again only for the ids
   * still unresolved. One attempt sends each coordinator its pending ids, settles each id from the
   * answer it got (see `rank`) and keeps the ids it settled for good; an id that was answered 14,
   * 15 or 16 stays pending, and so does every id of a request that threw, while the ids of the other
   * requests of the attempt keep their answers. The next attempt, after the retrier's backoff, finds
   * the coordinator again for the ids whose coordinator moved (15, 16) and re-groups the pending
   * ids by coordinator, so a move that splits ids across brokers is followed; an id that was loading
   * (14) is asked again where it is. The budget is the call's own: the attempts of this loop, whose
   * last failure is what the caller sees when it runs out.
   * @private
   * @param {string[]} ids the written ids, each once
   * @param {object} context captured operation and per-call coordinator map
   * @param {(coordinator: object, ids: string[]) => Promise<Array<{ groupId: string, value?: any, retry?: number }>>} ask
   *   one request for `ids`; a cell has either the settled `value` for its group id, or the
   *   `retry` code that leaves the id pending. An id of `ids` it gives no cell is left unresolved
   *   without a retry; a cell for an id not in `ids` is kept as it is
   * @returns {Promise<any[]>} the settled values, in the order the attempts settled them: each
   *   attempt in coordinator order, then in the order of each answer
   * @throws {Error} the last retriable code as an error once the budget is spent, and whatever a
   *   request, a lookup, the retrier or the operation raises
   */
  async #resolve(ids, context, ask) {
    const requested = new Set(ids)
    const pending = new Set(ids)
    const settled = new Map()
    let moved = false
    this.#checked(
      context,
      await this.#wait(context, async () =>
        this.#retrier(context)(async () => {
          if (moved) {
            moved = false
            this.#checked(
              context,
              await this.#wait(context, async () =>
                this.cluster.refreshMetadata(
                  ...(context.consumerOperation ? [{ fence: context.consumerOperation }] : [])
                )
              )
            )
          }
          const batches = this.#checked(
            context,
            await this.#wait(context, async () => this.#groupByCoordinator([...pending], context))
          )
          const answers = new Array(batches.length)
          let failure
          let failed = false
          this.#checked(
            context,
            await this.#wait(context, async () =>
              Promise.allSettled(
                batches.map(async ({ coordinator, ids: batchIds }, index) => {
                  try {
                    answers[index] = this.#checked(
                      context,
                      await this.#wait(context, async () => ask(coordinator, batchIds))
                    )
                  } catch (error) {
                    if (!failed) {
                      failed = true
                      failure = error
                    }
                    if (RE_FINDS_COORDINATOR.has(error.code)) {
                      for (const id of batchIds) context.coordinators.delete(id)
                    }
                    throw error
                  }
                })
              )
            )
          )
          let retryCode
          batches.forEach(({ ids: batchIds }, index) => {
            if (answers[index] === undefined) return
            for (const id of batchIds) pending.delete(id)
            for (const cell of answers[index]) {
              if (cell.retry === undefined) {
                settled.set(cell.groupId, cell.value)
              } else if (requested.has(cell.groupId)) {
                pending.add(cell.groupId)
                retryCode ??= cell.retry
                if (RE_FINDS_COORDINATOR.has(cell.retry)) {
                  context.coordinators.delete(cell.groupId)
                  moved = true
                }
              }
            }
          })
          if (failed) throw failure
          if (pending.size > 0) throw createErrorFromCode(retryCode)
        })
      )
    )
    return [...settled.values()]
  }

  /**
   * One ConsumerGroupDescribe for `ids` on one coordinator: each id's row either shaped, marked
   * for the classic fallback, or left to be asked again. A coordinator that does not implement
   * key 69 sends every one of its ids to the fallback in one go, and so does an id the answer
   * leaves out, with no error to carry forward, rather than being dropped from the result.
   * @private
   * @param {object} coordinator
   * @param {string[]} ids
   * @param {object} context captured operation and requested authorization flag
   * @returns {Promise<Array<{ groupId: string, value?: object, retry?: number }>>}
   */
  async #askConsumer(coordinator, ids, context) {
    let answer
    try {
      answer = this.#checked(
        context,
        await this.#wait(context, async () =>
          coordinator.consumerGroupDescribe({
            groupIds: ids,
            ...(context.consumerOperation && { guard: context.consumerOperation.guard }),
            includeAuthorizedOperations: context.includeAuthorizedOperations,
          })
        )
      )
    } catch (e) {
      if (!doesNotSupportApiKey(e)) throw e
      this.logger.debug('Broker does not offer ConsumerGroupDescribe, describing as classic', {
        nodeId: coordinator.nodeId,
        groupIds: ids,
      })
      return ids.map((groupId) => ({ groupId, value: { groupId, needsFallback: true } }))
    }

    const settled = settle(answer.groups.map((row) => this.#routeRow(row, context)))
    const cells = [...settled.values()].map((entry) =>
      entry.retry === undefined
        ? { groupId: entry.groupId, value: entry }
        : { groupId: entry.groupId, retry: entry.retry }
    )
    for (const groupId of ids) {
      if (!settled.has(groupId)) cells.push({ groupId, value: { groupId, needsFallback: true } })
    }
    return cells
  }

  /**
   * What to do with one ConsumerGroupDescribe row: shape it, route it to the older API, or ask
   * again. `parse` returns group-level codes rather than throwing them, so a coordinator that is
   * still loading (14) or has moved (15, 16) is marked `retry`.
   * @private
   * @param {object} row
   * @param {object} context captured operation and requested authorization flag
   * @returns {{ groupId: string, retry?: number, row?: object, needsFallback: boolean, error?: object }}
   */
  #routeRow(row, context) {
    const { groupId, errorCode, errorMessage } = row

    if (RETRIES_BATCH.has(errorCode)) {
      return { groupId, needsFallback: false, retry: errorCode }
    }

    if (ROUTES_TO_CLASSIC.has(errorCode)) {
      // Only a 69 is stashed, because a double miss reports its message; a 35 is about the cluster,
      // so the older API's own answer is what the caller sees.
      return {
        groupId,
        needsFallback: true,
        ...(errorCode === GROUP_ID_NOT_FOUND && { error: { errorCode, errorMessage } }),
      }
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
   * One DescribeGroups for `ids` on one coordinator. Its own parse throws every group-level code
   * but GROUP_ID_NOT_FOUND, so a row is never a reason to ask again; a throw is.
   * @private
   * @param {object} coordinator
   * @param {string[]} ids
   * @param {object} context captured operation and requested authorization flag
   * @returns {Promise<Array<{ groupId: string, value: object }>>} the raw key-15 rows
   */
  async #askClassic(coordinator, ids, context) {
    const { groups } = this.#checked(
      context,
      await this.#wait(context, async () =>
        coordinator.describeGroups({
          groupIds: ids,
          ...(context.consumerOperation && { guard: context.consumerOperation.guard }),
          includeAuthorizedOperations: context.includeAuthorizedOperations,
        })
      )
    )
    return groups.map((row) => ({ groupId: row.groupId, value: row }))
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
