const { MemberAssignment } = require('../consumer/assignerProtocol')
const { createErrorFromCode } = require('../protocol/error')
const { UNCHECKED_ROWS } = require('../broker/privateOptions')
const { isRetriable } = require('../retry')
const {
  retrierOf,
  currentDeadline,
  withConsumerOperation,
  endsCall,
  isAnsweredError,
} = require('./deadline')
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
/** Codes that mean an id's coordinator moved: find it again before the next attempt. */
const RE_FINDS_COORDINATOR = new Set([COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR])
/**
 * Codes on a ConsumerGroupDescribe row that leave that id unresolved, to be asked again on the
 * retry budget's backoff: where it is (14) or after finding its coordinator again (15, 16).
 */
const ASKS_AGAIN = new Set([COORDINATOR_LOAD_IN_PROGRESS, ...RE_FINDS_COORDINATOR])

/**
 * Whether a failure of one lookup or request ends the call: an error the retrier does not retry
 * (`retriable: false`, such as GROUP_AUTHORIZATION_FAILED, or a programming error), or one the
 * admin call treats as final. An attempt throws such a failure ahead of any retriable one, so the
 * call ends with it whichever request of the attempt finished first.
 * @param {Error} error
 * @returns {boolean}
 */
const endsDescribe = (error) => !isRetriable(error) || endsCall(error) || isAnsweredError(error)

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
   * @throws {KafkaJSNonRetriableError} carrying the code, when a lookup or request fails in a way
   *         that is not retried: for an id that fell back to DescribeGroups, any group-level code
   *         that API reports but GROUP_ID_NOT_FOUND (v6), 14, 15 and 16 (an authorization failure,
   *         say). Such a failure ends the call whichever request of the attempt finished first,
   *         and nothing is sent again. A group-level error the newer API reports stays on that
   *         group's row instead
   * @throws {KafkaJSNumberOfRetriesExceeded} when ids are still unresolved once the retry budget
   *         is spent; the cause is the failure the last attempt threw (see `#resolve`)
   *
   *         Each written id (a lone surrogate is written as U+FFFD, so the ids a caller gives can
   *         be written alike) is sent once per attempt and routed once, whatever spellings and
   *         repeats were given, and settles ONE outcome from the answer of the request that asked
   *         for it: a row for an id that request did not ask is ignored. Of several rows for one id,
   *         a described group beats an error row, an error row beats a 14, 15 or 16, that beats a
   *         69 or 35, and the first of equal rank wins. An id that was answered 14, 15 or 16, whose
   *         request failed or whose coordinator could not be found is the only one asked again; ids
   *         already settled keep their answer, and only the ids that moved are looked up again. The
   *         outcome is then given to the caller's spellings under each exact spelling, a well-formed
   *         one never renamed. An outcome from the consumer protocol gets one row per occurrence of
   *         an id the caller gave (a repeated id has two rows); one from the classic API gets one
   *         row per distinct spelling. Each row beyond the first for an id is an independent copy:
   *         changing one never changes another
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
        this.#resolve(names.ids, context, this.#consumerApi(context))
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

    const fallbackIds = fallbacks.map(({ groupId }) => groupId)
    const classic = this.#checked(
      context,
      await this.#wait(context, async () =>
        this.#resolve(fallbackIds, context, this.#classicApi(context))
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
   * Resolves every id through one API, and asks again only for the ids still unresolved, on ONE
   * counted retry budget and the call's own deadline. This loop holds the one ledger of the call
   * phase: which ids are pending and the one settled value of each settled id.
   *
   * One attempt finds the coordinator of every pending id and sends each coordinator one request
   * for its ids (`#request`), and decides nothing until every lookup and request of the attempt has
   * settled, so no work of an attempt outlives it. Each id of a request then has one outcome:
   * settled for good, or a failure that leaves it pending; an id the coordinator answered with no
   * row is done without a value. A failure coded 15 or 16, on a row or from a request, drops that
   * id's coordinator, and the next attempt refreshes metadata and looks it up again; after any
   * other failure (14 included) the id is asked again where it is. An id whose lookup failed stays
   * pending with that failure.
   *
   * While any id is pending the attempt throws one failure: the first that ends the call
   * (`endsDescribe`) if there is one, so the call ends with it whichever request finished first and
   * its ids are never sent again; otherwise the first lookup failure, else the first failure in
   * coordinator order, which the retrier counts and backs off on. Because the next attempt waits for every request of this one, a slow
   * coordinator delays asking another coordinator's ids again, by at most that request's timeout.
   * @private
   * @param {string[]} ids the written ids, each once
   * @param {object} context captured operation and per-call coordinator map
   * @param {{ ask: Function, outcomes: Function }} api how to ask one coordinator and read its rows
   *   (see `#consumerApi` and `#classicApi`)
   * @returns {Promise<any[]>} the settled values, in the order the attempts settled them: each
   *   attempt in coordinator order, then in the order of each answer
   * @throws {Error} the failure that ended the call, the retrier's exhaustion around the last
   *   attempt's failure, or the operation's refusal
   */
  async #resolve(ids, context, api) {
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
          const { batches, unlocated } = this.#checked(
            context,
            await this.#wait(context, async () => this.#groupByCoordinator([...pending], context))
          )
          const requests = this.#checked(
            context,
            await this.#wait(context, async () =>
              Promise.allSettled(
                batches.map(({ coordinator, ids: batchIds }) =>
                  this.#request(coordinator, batchIds, context, api)
                )
              )
            )
          )
          const broken = requests.find(({ status }) => status === 'rejected')
          if (broken) throw broken.reason

          const failures = unlocated
          batches.forEach(({ ids: batchIds }, index) => {
            const outcomes = requests[index].value
            for (const [groupId, outcome] of outcomes) {
              if (outcome.error === undefined) {
                settled.set(groupId, outcome.value)
                pending.delete(groupId)
                continue
              }
              failures.push(outcome.error)
              if (RE_FINDS_COORDINATOR.has(outcome.error.code)) {
                context.coordinators.delete(groupId)
                moved = true
              }
            }
            for (const groupId of batchIds) {
              if (!outcomes.has(groupId)) pending.delete(groupId)
            }
          })
          if (failures.length > 0) throw failures.find(endsDescribe) ?? failures[0]
        })
      )
    )
    return [...settled.values()]
  }

  /**
   * One request to one coordinator for a known set of ids, and one outcome for each id it answers.
   * Only rows for the ids this request asked are read: a coordinator answers for the ids it was
   * asked, so a row for any other id is ignored and can neither settle, re-route nor re-queue an
   * id. A request that fails gives every one of its ids that failure, so the ledger classifies it
   * with every other failure of the attempt; the call's own refusal ends the call instead.
   * @private
   * @param {object} coordinator
   * @param {string[]} ids the ids sent, each once
   * @param {object} context captured operation and requested authorization flag
   * @param {{ ask: Function, outcomes: Function }} api
   * @returns {Promise<Map<string, { value?: any, error?: Error }>>} outcomes for ids of `ids` only,
   *   in answer order; an id left out has no outcome
   * @throws {Error} the operation's refusal
   */
  async #request(coordinator, ids, context, api) {
    let rows
    try {
      rows = this.#checked(
        context,
        await this.#wait(context, async () => api.ask(coordinator, ids))
      )
    } catch (error) {
      context.operation?.check()
      return new Map(ids.map((groupId) => [groupId, { error }]))
    }
    const asked = new Set(ids)
    const foreign = rows.filter(({ groupId }) => !asked.has(groupId))
    if (foreign.length > 0) {
      this.logger.debug('Ignoring answer rows for group ids the request did not ask', {
        nodeId: coordinator.nodeId,
        groupIds: foreign.map(({ groupId }) => groupId),
      })
    }
    return api.outcomes(
      foreign.length > 0 ? rows.filter(({ groupId }) => asked.has(groupId)) : rows,
      ids
    )
  }

  /**
   * The newer API: ConsumerGroupDescribe, settled per id (see `settle`), with the classic fallback
   * for the ids it does not describe.
   * @private
   * @param {object} context captured operation and requested authorization flag
   * @returns {{ ask: Function, outcomes: Function }}
   */
  #consumerApi(context) {
    return {
      ask: (coordinator, ids) => this.#askConsumer(coordinator, ids, context),
      outcomes: (rows, ids) => this.#consumerOutcomes(rows, ids, context),
    }
  }

  /**
   * The older API: DescribeGroups, read row by row.
   * @private
   * @param {object} context captured operation and requested authorization flag
   * @returns {{ ask: Function, outcomes: Function }}
   */
  #classicApi(context) {
    return {
      ask: (coordinator, ids) => this.#askClassic(coordinator, ids, context),
      outcomes: (rows) => this.#classicOutcomes(rows),
    }
  }

  /**
   * One ConsumerGroupDescribe for `ids` on one coordinator. A coordinator that does not implement
   * key 69 answers no row, so every one of its ids goes to the fallback (see `#consumerOutcomes`).
   * @private
   * @param {object} coordinator
   * @param {string[]} ids
   * @param {object} context captured operation and requested authorization flag
   * @returns {Promise<object[]>} the decoded key-69 rows
   */
  async #askConsumer(coordinator, ids, context) {
    try {
      const { groups } = this.#checked(
        context,
        await this.#wait(context, async () =>
          coordinator.consumerGroupDescribe({
            groupIds: ids,
            ...(context.consumerOperation && { guard: context.consumerOperation.guard }),
            includeAuthorizedOperations: context.includeAuthorizedOperations,
          })
        )
      )
      return groups
    } catch (e) {
      if (!doesNotSupportApiKey(e)) throw e
      this.logger.debug('Broker does not offer ConsumerGroupDescribe, describing as classic', {
        nodeId: coordinator.nodeId,
        groupIds: ids,
      })
      return []
    }
  }

  /**
   * One outcome per id from ConsumerGroupDescribe rows: the settled entry (see `settle`), shaped
   * or marked for the classic fallback, or the 14, 15 or 16 that leaves the id to be asked again.
   * An id the answer leaves out is marked for the fallback too, with no error to carry forward,
   * rather than being dropped from the result.
   * @private
   * @param {object[]} rows the rows for ids the request asked
   * @param {string[]} ids the ids the request asked
   * @param {object} context captured operation and requested authorization flag
   * @returns {Map<string, { value?: object, error?: Error }>}
   */
  #consumerOutcomes(rows, ids, context) {
    const outcomes = new Map()
    for (const entry of settle(rows.map((row) => this.#routeRow(row, context))).values()) {
      outcomes.set(
        entry.groupId,
        entry.retry === undefined ? { value: entry } : { error: createErrorFromCode(entry.retry) }
      )
    }
    for (const groupId of ids) {
      if (!outcomes.has(groupId)) outcomes.set(groupId, { value: { groupId, needsFallback: true } })
    }
    return outcomes
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

    if (ASKS_AGAIN.has(errorCode)) {
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
   * One DescribeGroups for `ids` on one coordinator, its rows read one by one: a row the version's
   * own rule refuses (every group-level code but GROUP_ID_NOT_FOUND from v6, every one below it)
   * carries that error instead of failing the request, so the other rows of the answer still count.
   * @private
   * @param {object} coordinator
   * @param {string[]} ids
   * @param {object} context captured operation and requested authorization flag
   * @returns {Promise<object[]>} the decoded key-15 rows, a refused one with its `error`
   */
  async #askClassic(coordinator, ids, context) {
    const { groups } = this.#checked(
      context,
      await this.#wait(context, async () =>
        coordinator.describeGroups(
          {
            groupIds: ids,
            ...(context.consumerOperation && { guard: context.consumerOperation.guard }),
            includeAuthorizedOperations: context.includeAuthorizedOperations,
          },
          { [UNCHECKED_ROWS]: true }
        )
      )
    )
    return groups
  }

  /**
   * One outcome per id from DescribeGroups rows: the row itself, or the error it carries, which the
   * ledger classifies (14 asks again where it is, 15 and 16 after finding the coordinator again, an
   * authorization failure ends the call). The request asked each id once, so of two rows for one id
   * the first is the answer.
   * @private
   * @param {object[]} rows the rows for ids the request asked
   * @returns {Map<string, { value?: object, error?: Error }>}
   */
  #classicOutcomes(rows) {
    const outcomes = new Map()
    for (const row of rows) {
      if (outcomes.has(row.groupId)) continue
      outcomes.set(row.groupId, row.error === undefined ? { value: row } : { error: row.error })
    }
    return outcomes
  }

  /**
   * FindCoordinator for every id, then one batch per distinct coordinator node - the shape this
   * client has always used, kept so N ids do not become N round trips. Every lookup settles before
   * this returns, so a lookup never outlives the attempt that started it; an id whose lookup failed
   * is in no batch, and its failure is returned.
   * @private
   * @param {string[]} groupIds
   * @param {object} context captured operation and per-call coordinator map
   * @returns {Promise<{ batches: Array<{ coordinator: object, ids: string[] }>, unlocated: Error[] }>}
   */
  async #groupByCoordinator(groupIds, context) {
    const located = this.#checked(
      context,
      await this.#wait(context, async () =>
        Promise.allSettled(
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
    const unlocated = []
    for (const outcome of located) {
      if (outcome.status === 'rejected') {
        unlocated.push(outcome.reason)
        continue
      }
      const { groupId, coordinator } = outcome.value
      const batch = batches.get(coordinator.nodeId) || { coordinator, ids: [] }
      batch.ids.push(groupId)
      batches.set(coordinator.nodeId, batch)
    }

    return { batches: [...batches.values()], unlocated }
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
