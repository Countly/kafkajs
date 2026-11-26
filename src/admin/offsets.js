const {
  KafkaJSAggregateError,
  KafkaJSError,
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSProtocolError,
} = require('../errors')
const { createErrorFromCode, failure } = require('../protocol/error')
const { ON_DECODED, UNCHECKED_ROWS } = require('../broker/privateOptions')
const { itemRetry, isLocal } = require('./itemRetry')
const { checked, waitOf, currentDeadline, endsCall, isAnsweredError } = require('./deadline')
const { exact, validateTopics } = require('../protocol/requests/offsetFetch/responseIdentity')
const apiKeys = require('../protocol/requests/apiKeys')
const { requests } = require('../protocol/requests')

/** Refuses new-method input before any lookup, naming only the invalid field.
 * @param {boolean} valid validation predicate
 * @param {string} field public input name
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} invalid input
 */
const requireInput = (valid, field) => {
  if (!valid) throw new KafkaJSInvalidConfigurationError(`Invalid ${field}`)
}
/** Validates a namespaced identifier without echoing caller-controlled input.
 * @param {unknown} value name
 * @returns {boolean} valid nonempty name without NUL
 */
const name = (value) => typeof value === 'string' && value.length > 0 && !value.includes('\0')
/** Validates an options object.
 * @param {unknown} options supplied options
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} invalid options
 */
const object = (options) =>
  requireInput(
    options !== null && typeof options === 'object' && !Array.isArray(options),
    'options'
  )
/** Validates an array and its unique identifiers.
 * @param {unknown} rows supplied list
 * @param {Function} identity validated identity extractor
 * @param {string} field public field name
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} invalid list or duplicate
 */
const unique = (rows, identity, field) => {
  requireInput(Array.isArray(rows), field)
  const seen = new Set()
  for (const row of rows) {
    const key = identity(row)
    requireInput(key !== undefined && !seen.has(key), field)
    seen.add(key)
  }
}
/** Normalizes topic selections; omitted read filters mean all and empty filters mean no work.
 * @param {object[]|undefined} topics public selection
 * @param {boolean} mutation deletion requires nonempty explicit partitions
 * @returns {object[]|null} wire-shaped topic/partition selection
 * @throws {KafkaJSInvalidConfigurationError} malformed or duplicate selection
 */
const selection = (topics, mutation) => {
  if (topics === undefined && !mutation) return null
  unique(topics, (row) => (row && name(row.topic) ? row.topic : undefined), 'topics')
  if (mutation) requireInput(topics.length > 0, 'topics')
  return topics
    .map((row) => {
      if (row.partitions === undefined && !mutation) return { topic: row.topic, partitions: null }
      unique(
        row.partitions,
        (p) => (Number.isInteger(p) && p >= 0 && p <= 2147483647 ? p : undefined),
        'partitions'
      )
      if (mutation) requireInput(row.partitions.length > 0, 'partitions')
      return { topic: row.topic, partitions: row.partitions.map((partition) => ({ partition })) }
    })
    .filter((row) => row.partitions === null || row.partitions.length > 0)
}
/** Groups partition cells in caller order, without object-key collisions for special topic names.
 * @param {object[]} cells flat cells with topic and partition
 * @returns {object[]} nested topics
 */
const byTopic = (cells) => {
  const topics = new Map()
  for (const { topic, ...cell } of cells) {
    if (!topics.has(topic)) topics.set(topic, { topic, partitions: [] })
    topics.get(topic).partitions.push(cell)
  }
  return [...topics.values()]
}
/** Gives a cell an unambiguous identity.
 * @param {object} cell topic and partition
 * @returns {string} tuple identity
 */
const cellKey = (cell) => JSON.stringify([cell.topic, cell.partition])
/** Finds an original call-level refusal through inherited retry wrappers.
 * @param {Error} error local failure, possibly carrying a cause chain
 * @returns {Error|null} original validation, capability, auth or identity refusal
 */
const failureOf = (error) => {
  const seen = new Set()
  for (let current = error; current && !seen.has(current); current = current.cause) {
    seen.add(current)
    if (
      current instanceof KafkaJSInvariantViolation ||
      [
        'KafkaJSInvalidConfigurationError',
        'KafkaJSServerDoesNotSupportApiKey',
        'KafkaJSSASLAuthenticationError',
      ].includes(current.name)
    )
      return current
  }
  return null
}
/** Classifies call-level local failures; broker rows use numeric family rules.
 * @param {Error} error local failure
 * @returns {boolean} original refusal is present
 */
const callFailure = (error) => failureOf(error) !== null
/** Adds stable per-item error fields without replacing raw numeric codes.
 * @param {object} row broker result
 * @returns {object} public item fields
 */
const resultOf = (row) => ({
  ...row,
  error: row.errorCode === 0 ? null : createErrorFromCode(row.errorCode),
  errorMessage: row.errorMessage ?? null,
})

/** OffsetCommit codes sent again, as the Java admin's `AlterConsumerGroupOffsetsHandler` does. */
const COMMIT_RETRIES = [14, 15, 16, 27]
/** OffsetCommit codes after which the coordinator is looked up again. */
const COORDINATOR_MOVED = [15, 16]

/** Whether an OffsetCommit rejection is the parser's throw for the first failed partition of the
 * reply it decoded, rather than a transport, deadline or decode failure. Matched by topic and
 * partition, since a code this client does not know is thrown under another code.
 * @param {Error} error what the request rejected with
 * @param {{responses: object[]}|null} decoded the reply this attempt decoded, if any
 * @returns {boolean}
 */
const isFirstFailedPartition = (error, decoded) => {
  if (!decoded || !(error instanceof KafkaJSProtocolError)) return false
  for (const { topic, partitions } of decoded.responses) {
    const failed = partitions.find((partition) => failure(partition.errorCode))
    if (failed) return error.topic === topic && error.partition === failed.partition
  }
  return false
}

/** Rejects unless every committed row was accepted.
 * @param {object[]} rows itemRetry results: errorCode 0, a broker code, or a local failure row
 * @returns {void}
 * @throws {Error} the local failure itself when no partition was confirmed or answered with a code;
 *   otherwise a `KafkaJSAggregateError` with one child per partition not committed
 */
const assertCommitted = (rows) => {
  const failed = rows.filter((row) => row.errorCode !== 0)
  if (failed.length === 0) return
  if (failed.length === rows.length && failed.every(isLocal)) throw failed[0].error
  const children = failed.map((row) => {
    const child = isLocal(row)
      ? new KafkaJSError(`Offset not confirmed: ${row.error.message}`, {
          retriable: row.error.retriable !== false,
          cause: row.error,
        })
      : row.error
    return Object.assign(child, { topic: row.topic, partition: row.partition })
  })
  const causes = failed.map((row) =>
    isLocal(row)
      ? `${row.partition} (not confirmed: ${row.error.name})`
      : `${row.partition} (${row.error.type})`
  )
  throw new KafkaJSAggregateError(
    `Offsets not committed for ${failed.length} of ${rows.length} partitions: ${causes.join(', ')}`,
    children
  )
}

/** Implements coordinator-routed offset calls under the enclosing admin invocation budget. */
class OffsetAdmin {
  /**
   * @param {object} options shared cluster and unchanged retry options
   * @returns {OffsetAdmin} stateless family; all maps belong to individual calls
   */
  constructor({ cluster, retry }) {
    this.cluster = cluster
    this.retry = retry
  }

  /** Deletes offsets with per-partition results. A coordinator still loading the group (a
   * whole-response 14) is asked again within the retry budget; every other answer is final.
   * @param {object} options groupId, topics and inherited apiTimeout
   * @returns {Promise<{topics:object[]}>} caller-ordered partition outcomes
   * @throws {Error} invalid input, API deadline, group refusal or before-work capability/auth
   */
  async deleteOffsets(options) {
    object(options)
    const { groupId } = options
    requireInput(name(groupId), 'groupId')
    const topics = selection(options.topics, true)
    const items = topics.flatMap(({ topic, partitions }) =>
      partitions.map((p) => ({ topic, ...p }))
    )
    let coordinator
    let worked = false
    let groupFailure
    const loading = new WeakSet()
    const rows = await itemRetry(
      {
        items,
        keyOf: cellKey,
        retry: this.retry,
        retryable: (row) => loading.has(row),
        callFailure: (error) =>
          [30, 69, 24, 68].includes(error?.code) ||
          (!worked && callFailure(error)) ||
          failureOf(error) instanceof KafkaJSInvariantViolation,
        sanitize: (message) => message,
      },
      async (pending) => {
        coordinator ||= checked(await waitOf(() => this.cluster.findGroupCoordinator({ groupId })))
        const response = checked(
          await waitOf(() => coordinator.offsetDelete({ groupId, topics: byTopic(pending) }))
        )
        worked = true
        if ([30, 69, 24, 68].includes(response.errorCode)) {
          groupFailure = createErrorFromCode(response.errorCode)
          throw groupFailure
        }
        if (response.errorCode !== 0) {
          if ([15, 16].includes(response.errorCode)) coordinator = null
          const rows = pending.map((item) => ({ ...item, errorCode: response.errorCode }))
          if (response.errorCode === 14) for (const row of rows) loading.add(row)
          return rows
        }
        validateTopics({ errorCode: 0, topics: response.topics }, byTopic(pending))
        const rows = response.topics.flatMap(({ topic, partitions }) =>
          partitions.map((p) => ({ topic, ...p }))
        )
        if (rows.some((p) => [15, 16].includes(p.errorCode))) coordinator = null
        return rows
      }
    ).catch((error) => {
      checked(null)
      if (endsCall(error) || isAnsweredError(error)) throw error
      if (groupFailure && error.code === groupFailure.code) throw groupFailure
      throw failureOf(error) ?? error
    })
    return { topics: byTopic(rows) }
  }

  /**
   * Commits offsets of one topic as the group's, with one OffsetCommit that carries no member id,
   * generation -1 and no instance id, as the Java admin's `alterConsumerGroupOffsets` does. The
   * coordinator accepts it only while the group has no members (otherwise `UNKNOWN_MEMBER_ID`), and
   * creates a missing group. Partitions answered 14, 15, 16 or 27 are sent again within the retry
   * budget, alone, after a new coordinator lookup on 15 or 16; a request that got no answer is sent
   * again after a new lookup. A confirmed partition is never sent again by this call. Every other
   * code is final. Each cell carries only its partition and offset, so the commit stores null
   * metadata and leader epoch -1.
   * @param {object} options
   * @param {string} options.groupId
   * @param {string} options.topic
   * @param {Array<{partition: number, offset: string}>} options.cells one per partition
   * @returns {Promise<void>} once the coordinator accepted every cell
   * @throws {KafkaJSAggregateError} one child per partition not committed: the coordinator's
   *   `KafkaJSProtocolError` with `topic` and `partition`, or, for a partition whose last attempt
   *   got no answer, a `KafkaJSError` carrying that failure as `cause`. Partitions not listed were
   *   committed.
   * @throws {Error} the request's own failure (a timeout, say) when no partition got a code
   * @throws {KafkaJSInvariantViolation} for a reply that leaves out, repeats or adds a partition
   * @throws {KafkaJSTimeout} once the API deadline passed; earlier attempts may have been applied
   */
  async commitOffsets({ groupId, topic, cells }) {
    let coordinator = null
    let worked = false
    const rows = await itemRetry(
      {
        items: cells.map(({ partition, offset }) => ({ topic, partition, offset })),
        keyOf: cellKey,
        retry: this.retry,
        retryable: (row) => COMMIT_RETRIES.includes(row.errorCode),
        callFailure: (error) => !worked && callFailure(error),
        sanitize: (message) => message,
      },
      async (pending) => {
        coordinator ||= checked(await waitOf(() => this.cluster.findGroupCoordinator({ groupId })))
        const target = coordinator
        let decoded = null
        // `itemRetry` refuses a missing, repeated or foreign row itself, as an invariant violation.
        const options = { [ON_DECODED]: (reply) => (decoded = reply), [UNCHECKED_ROWS]: true }
        const request = {
          groupId,
          groupGenerationId: -1,
          memberId: '',
          groupInstanceId: null,
          topics: byTopic(pending),
        }
        try {
          checked(await waitOf(() => target.offsetCommit(request, options)))
        } catch (error) {
          if (!isFirstFailedPartition(error, decoded)) {
            if (!decoded) coordinator = null
            throw error
          }
        }
        worked = true
        const rows = (decoded?.responses ?? []).flatMap(({ topic, partitions }) =>
          partitions.map(({ partition, errorCode }) => ({ topic, partition, errorCode }))
        )
        if (rows.some((row) => COORDINATOR_MOVED.includes(row.errorCode))) coordinator = null
        return rows
      }
    ).catch((error) => {
      throw failureOf(error) ?? error
    })
    assertCommitted(rows)
  }

  /** Resolves explicitly named topics with omitted partition filters before OffsetFetch.
   * @param {object[]|null} topics normalized selection
   * @returns {Promise<object[]|null>} explicit partition cells or all-topic marker
   * @throws {Error} metadata/deadline refusal
   */
  async resolve(topics) {
    if (topics === null) return null
    return checked(
      await waitOf(() =>
        Promise.all(
          topics.map(async (row) => {
            if (row.partitions !== null) return row
            checked(await waitOf(() => this.cluster.addTargetTopic(row.topic)))
            checked(await waitOf(() => this.cluster.refreshMetadataIfNecessary()))
            return {
              topic: row.topic,
              partitions: this.cluster
                .findTopicPartitionMetadata(row.topic)
                .map((p) => ({ partition: p.partitionId })),
            }
          })
        )
      )
    ).filter((row) => row.partitions.length > 0)
  }

  /** Restores explicit caller partition order while retaining broker order for all-topic reads.
   * @param {object} state call-local group state
   * @returns {object[]} observed topic results
   */
  ordered(state) {
    if (state.original === null) return byTopic([...state.cells.values()])
    const cells = state.original
      .flatMap(({ topic, partitions }) =>
        partitions.map((p) => state.cells.get(cellKey({ topic, ...p })))
      )
      .filter(Boolean)
    return byTopic(cells)
  }

  /** Fetches multiple groups with one batched request per coordinator when v8+ is supported.
   * Successful and terminal partitions are retained locally, never re-sent on stable retries.
   * @param {object} options unique groups, optional requireStable and inherited apiTimeout
   * @returns {Promise<object[]>} one data-result entry per group in caller order
   * @throws {Error} input/identity/deadline failure or before-work capability/auth
   */
  async fetchOffsetsForGroups(options) {
    object(options)
    const { groups, requireStable = false } = options
    requireInput(typeof requireStable === 'boolean', 'requireStable')
    unique(groups, (g) => (g && name(g.groupId) ? g.groupId : undefined), 'groups')
    requireInput(groups.length > 0, 'groups')
    const normalized = groups.map((g) => ({
      groupId: g.groupId,
      topics: selection(g.topics, false),
    }))
    const resolved = checked(
      await waitOf(() =>
        Promise.all(normalized.map(async (g) => ({ ...g, topics: await this.resolve(g.topics) })))
      )
    )
    const states = new Map(
      resolved.map((g) => [
        g.groupId,
        { ...g, original: g.topics, coordinator: null, cells: new Map(), last: null },
      ])
    )
    const empty = new Map(
      resolved
        .filter((g) => g.topics?.length === 0)
        .map((g) => [g.groupId, { ...resultOf({ groupId: g.groupId, errorCode: 0 }), topics: [] }])
    )
    const unown = currentDeadline()?.own(() => states.clear())
    try {
      checked(null)
      let worked = false
      const rows = await itemRetry(
        {
          items: resolved.filter((g) => !empty.has(g.groupId)),
          keyOf: (g) => g.groupId,
          retry: this.retry,
          retryable: (row) => row.retry === true,
          callFailure: (error) =>
            failureOf(error) instanceof KafkaJSInvariantViolation ||
            (!worked && callFailure(error)),
          sanitize: (message) => message,
        },
        async (pending) => {
          const batches = new Map()
          for (const group of pending) {
            const state = states.get(group.groupId)
            try {
              state.coordinator ||= checked(
                await waitOf(() => this.cluster.findGroupCoordinator({ groupId: group.groupId }))
              )
            } catch (error) {
              checked(null)
              batches.set(group, { groups: [group], error })
              continue
            }
            const key = state.coordinator.nodeId ?? state.coordinator
            if (!batches.has(key)) batches.set(key, { broker: state.coordinator, groups: [] })
            batches.get(key).groups.push({ groupId: group.groupId, topics: state.topics })
          }
          const batchList = [...batches.values()]
          const outcomes = checked(
            await waitOf(() =>
              Promise.allSettled(
                batchList.map(async ({ broker, groups: batch, error }) => {
                  if (error) throw error
                  const version = broker.lookupRequestWithMax(
                    apiKeys.OffsetFetch,
                    requests.OffsetFetch,
                    9
                  )({ groupId: batch[0].groupId, topics: [] }).request.apiVersion
                  if (version < 8) {
                    const single = checked(
                      await waitOf(() =>
                        Promise.allSettled(
                          batch.map(async (g) => {
                            const response = checked(
                              await waitOf(() =>
                                broker.offsetFetch({ ...g, requireStable, returnErrors: true })
                              )
                            )
                            return {
                              groupId: g.groupId,
                              errorCode: response.errorCode ?? 0,
                              topics: response.responses,
                            }
                          })
                        )
                      )
                    )
                    return single.map((outcome, index) => {
                      if (outcome.status === 'fulfilled') return outcome.value
                      const error = outcome.reason
                      if (
                        failureOf(error) instanceof KafkaJSInvariantViolation ||
                        (!worked &&
                          !single.some((r) => r.status === 'fulfilled') &&
                          callFailure(error))
                      )
                        throw error
                      return {
                        groupId: batch[index].groupId,
                        errorCode: [14, 15, 16].includes(error.code) ? error.code : -1,
                        localError: error,
                        topics: [],
                      }
                    })
                  }
                  const response = checked(
                    await waitOf(() => broker.offsetFetch({ groups: batch, requireStable }))
                  )
                  exact(response.groups, batch, 'groupId', 'OffsetFetch group')
                  return response.groups
                })
              )
            )
          )
          const succeeded = outcomes.some((outcome) => outcome.status === 'fulfilled')
          const results = outcomes.map((outcome, index) => {
            if (outcome.status === 'fulfilled') return outcome.value
            const error = outcome.reason
            if (
              failureOf(error) instanceof KafkaJSInvariantViolation ||
              (!worked && !succeeded && callFailure(error))
            )
              throw error
            return batchList[index].groups.map((g) => ({
              groupId: g.groupId,
              errorCode: [14, 15, 16].includes(error.code) ? error.code : -1,
              localError: error,
              topics: [],
            }))
          })
          worked ||= succeeded
          return results.flat().map((group) => {
            const state = states.get(group.groupId)
            if (!state) throw new KafkaJSInvariantViolation('Unrequested OffsetFetch group')
            if (group.errorCode !== 0) {
              if ([15, 16].includes(group.errorCode)) state.coordinator = null
              const value = {
                ...resultOf(group),
                ...(group.localError
                  ? { error: group.localError, errorMessage: group.localError.message }
                  : {}),
                topics: this.ordered(state),
              }
              state.last = value
              return {
                groupId: group.groupId,
                errorCode: group.errorCode,
                retry: Boolean(group.localError) || [14, 15, 16].includes(group.errorCode),
                value,
              }
            }
            validateTopics(group, state.topics)
            const retry = []
            for (const { topic, partitions } of group.topics)
              for (const p of partitions) {
                const cell = {
                  topic,
                  ...resultOf(p),
                  leaderEpoch:
                    p.committedLeaderEpoch === -1 ? null : (p.committedLeaderEpoch ?? null),
                  metadata: p.metadata ?? null,
                }
                state.cells.set(cellKey(cell), cell)
                if (requireStable && p.errorCode === 88)
                  retry.push({ topic, partition: p.partition })
              }
            const value = {
              ...resultOf({ groupId: group.groupId, errorCode: 0 }),
              topics: this.ordered(state),
            }
            state.last = value
            if (retry.length) state.topics = byTopic(retry)
            return {
              groupId: group.groupId,
              errorCode: retry.length ? 88 : 0,
              retry: retry.length > 0,
              value,
            }
          })
        }
      )
      const done = new Map(
        rows.map((row) => {
          const state = states.get(row.groupId)
          if (row.errorCode === -1 && !row.value)
            return [
              row.groupId,
              {
                ...resultOf({ groupId: row.groupId, errorCode: -1 }),
                error: row.error,
                errorMessage: row.errorMessage,
                topics: state.last?.topics ?? [],
              },
            ]
          return [row.groupId, row.value]
        })
      )
      return groups.map((g) => empty.get(g.groupId) ?? done.get(g.groupId))
    } catch (error) {
      throw failureOf(error) ?? error
    } finally {
      unown?.()
      states.clear()
      empty.clear()
    }
  }
}

/** Creates family methods; the enclosing admin factory adds the single public deadline wrapper.
 * @param {object} options cluster and retry settings
 * @returns {object} deleteOffsets and fetchOffsetsForGroups methods
 */
const createOffsets = (options) => {
  const family = new OffsetAdmin(options)
  return {
    deleteOffsets: (options) => family.deleteOffsets(options),
    fetchOffsetsForGroups: (options) => family.fetchOffsetsForGroups(options),
  }
}
module.exports = { createOffsets, OffsetAdmin }
