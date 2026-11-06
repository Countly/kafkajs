const {
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSNotImplemented,
  KafkaJSError,
  KafkaJSProtocolError,
} = require('../errors')
const { createErrorFromCode } = require('../protocol/error')
const apiKeys = require('../protocol/requests/apiKeys')
const { responseSourceOf } = require('../broker/failureSource')
const COORDINATOR_TYPES = require('../protocol/coordinatorTypes')
const { itemRetry } = require('./itemRetry')
const { checked, waitOf, retrierOf, currentDeadline } = require('./deadline')

/** Transaction administration, independent of producer state and retry generations. */
class TransactionAdmin {
  /**
   * Captures shared routing collaborators; all result and route holders are call-local.
   * @param {object} options dependencies
   * @param {object} options.cluster existing admin cluster
   * @param {object} options.retry unchanged admin retry settings
   * @returns {TransactionAdmin} methods for the one public deadline wrapper
   */
  constructor({ cluster, retry }) {
    this.cluster = cluster
    this.retry = retry
  }

  /** Validates a new options object before any work.
   * @param {object} options public options @returns {object} options
   * @throws {KafkaJSInvalidConfigurationError} non-object options
   */
  options(options) {
    if (options === null || typeof options !== 'object' || Array.isArray(options))
      throw new KafkaJSInvalidConfigurationError('Transaction options must be an object')
    return options
  }

  /** Validates an integer without coercion.
   * @param {number} value public value @param {number} max inclusive maximum
   * @param {string} label field name @param {number} [min=0] inclusive minimum
   * @returns {number} value @throws {KafkaJSInvalidConfigurationError} invalid integer
   */
  integer(value, max, label, min = 0) {
    if (!Number.isSafeInteger(value) || value < min || value > max)
      throw new KafkaJSInvalidConfigurationError(`Invalid ${label}`)
    return value
  }

  /** Validates a name or broker filter without compiling patterns.
   * @param {string} value public string @param {boolean} [empty=false] allow empty filter
   * @returns {string} value @throws {KafkaJSInvalidConfigurationError} invalid string
   */
  text(value, empty = false) {
    if (typeof value !== 'string' || (!empty && value.length === 0) || value.includes('\0'))
      throw new KafkaJSInvalidConfigurationError('Invalid transaction string')
    return value
  }

  /** Normalizes one non-negative int64 once, without precision loss.
   * @param {string|number} value producer id @returns {string} canonical decimal
   * @throws {KafkaJSInvalidConfigurationError} unsafe number, syntax or range
   */
  producerId(value) {
    if (typeof value === 'number') {
      if (!Number.isSafeInteger(value) || value < 0)
        throw new KafkaJSInvalidConfigurationError('Invalid producerId')
      value = String(value)
    }
    if (
      typeof value !== 'string' ||
      !/^(0|[1-9][0-9]*)$/.test(value) ||
      value.length > 19 ||
      BigInt(value) > 9223372036854775807n
    )
      throw new KafkaJSInvalidConfigurationError('Invalid producerId')
    return value
  }

  /** Copies and validates a unique list before asynchronous routing begins.
   * @param {unknown[]} values list @param {Function} normalize element validator
   * @param {boolean} [empty=false] allow empty @returns {unknown[]} normalized copy
   * @throws {KafkaJSInvalidConfigurationError} empty, duplicate or invalid list
   */
  unique(values, normalize, empty = false) {
    if (!Array.isArray(values) || (!empty && values.length === 0))
      throw new KafkaJSInvalidConfigurationError('Invalid transaction list')
    const result = values.map(normalize)
    if (new Set(result).size !== result.length)
      throw new KafkaJSInvalidConfigurationError('Duplicate transaction identity')
    return result
  }

  /** Flattens caller partitions, preserving topic and partition order.
   * @param {object[]} topics public topicPartitions @returns {object[]} unique cells
   * @throws {KafkaJSInvalidConfigurationError} invalid or duplicate topic/partition
   */
  partitions(topics) {
    if (!Array.isArray(topics) || !topics.length)
      throw new KafkaJSInvalidConfigurationError('Invalid topicPartitions')
    const names = new Set()
    return topics.flatMap((entry) => {
      this.options(entry)
      const topic = this.text(entry.topic)
      if (names.has(topic)) throw new KafkaJSInvalidConfigurationError('Duplicate topic')
      names.add(topic)
      return this.unique(entry.partitions, (p) => this.integer(p, 2147483647, 'partition')).map(
        (partition) => ({ topic, partition, activeProducers: [] })
      )
    })
  }

  /** Creates an unambiguous compound key.
   * @param {object} item topic/partition or id cell @returns {string} identity
   */
  key(item) {
    return item.transactionalId === undefined
      ? JSON.stringify([item.topic, item.partition])
      : item.transactionalId
  }

  /** Separates whole-call failures from unresolved item failures.
   * @param {Error} error failure @returns {boolean} whether the call must reject
   */
  callFailure(error) {
    return (
      error instanceof KafkaJSInvalidConfigurationError ||
      error instanceof KafkaJSInvariantViolation ||
      error instanceof KafkaJSServerDoesNotSupportApiKey ||
      error instanceof KafkaJSNotImplemented
    )
  }

  /** Converts an unresolved item's failure to its result row. A route, connection-setup
   * (ApiVersions, SASL), transport or other local failure is code -1 with the original typed error,
   * even when that error carries a protocol code; only the item's own broker reply keeps its
   * numeric code for the family's retry policy.
   * @param {object} item requested cell @param {Error} error original failure
   * @param {boolean} [reply=false] whether the error is the broker's reply for this item
   * @returns {object} typed row @throws {Error} whole-call failure
   */
  failed(item, error, reply = false) {
    checked(null)
    if (this.callFailure(error)) throw error
    return {
      ...item,
      errorCode: reply ? error.code : -1,
      error,
      errorMessage: error.message ?? null,
    }
  }

  /** Requires one row per requested identity before accepting any response row.
   * @param {object[]} items requested cells @param {object[]} rows decoded cells
   * @returns {object[]} rows @throws {KafkaJSInvariantViolation} duplicate, missing or foreign row
   */
  exact(items, rows) {
    const expected = new Set(items.map((x) => this.key(x)))
    const seen = new Set()
    if (!Array.isArray(rows)) throw new KafkaJSInvariantViolation('Missing transaction results')
    for (const row of rows) {
      const key = this.key(row)
      if (!expected.has(key) || seen.has(key) || !Number.isInteger(row.errorCode))
        throw new KafkaJSInvariantViolation('Invalid transaction result identity')
      seen.add(key)
    }
    if (seen.size !== expected.size)
      throw new KafkaJSInvariantViolation('Missing transaction result')
    return rows
  }

  /** Groups flat partition cells for one broker request.
   * @param {object[]} items partition cells @returns {object[]} wire topics
   */
  topics(items) {
    const groups = new Map()
    for (const item of items) {
      if (!groups.has(item.topic)) groups.set(item.topic, { topic: item.topic, partitions: [] })
      groups.get(item.topic).partitions.push(item.partition)
    }
    return [...groups.values()]
  }

  /** Finds only pending routes. Per-item lookup failures do not discard another route's rows.
   * @param {object[]} items pending cells @param {Function} locate lazy route lookup
   * @param {Function} send one route's RPC and correlation
   * @returns {Promise<object[]>} complete attempted rows @throws {Error} whole-call failures
   */
  async batches(items, locate, send) {
    const batches = new Map()
    const rows = []
    for (const item of items) {
      try {
        const broker = checked(await waitOf(() => locate(item)))
        if (!batches.has(broker)) batches.set(broker, [])
        batches.get(broker).push(item)
      } catch (error) {
        rows.push(this.failed(item, error))
      }
    }
    for (const [broker, cells] of batches) {
      try {
        const response = checked(await waitOf(() => send(broker, cells)))
        rows.push(...this.exact(cells, response))
      } catch (error) {
        rows.push(...cells.map((cell) => this.failed(cell, error)))
      }
    }
    return rows
  }

  /** Retries pending items under the inherited deadline and unchanged retry count/backoff.
   * @param {object[]} items caller cells @param {Function} retryable family classifier
   * @param {Function} send pending sender @returns {Promise<object[]>} caller-ordered results
   * @throws {Error} deadline, malformed response, validation or capability failure
   */
  async items(items, retryable, send) {
    return itemRetry(
      {
        items,
        keyOf: (x) => this.key(x),
        retry: this.retry,
        retryable: (row, item) =>
          row.errorCode === -1 ? row.error?.retriable === true : retryable(row, item),
        callFailure: (error) => this.callFailure(error),
        sanitize: (message) => message,
      },
      send
    )
  }

  /** Finds one id's transaction coordinator once per call, until the id is relocated.
   * @param {string} transactionalId id @param {Map<string, object>} routes call-local routes
   * @returns {Promise<object>} coordinator broker @throws {Error} lookup or deadline failure
   */
  async coordinator(transactionalId, routes) {
    if (!routes.has(transactionalId))
      routes.set(
        transactionalId,
        checked(
          await waitOf(() =>
            this.cluster.findGroupCoordinator({
              groupId: transactionalId,
              coordinatorType: COORDINATOR_TYPES.TRANSACTION,
            })
          )
        )
      )
    return routes.get(transactionalId)
  }

  /** Routes one partition to its leader or an explicitly requested replica.
   * @param {object} item partition @param {number} [brokerId] fixed replica
   * @returns {Promise<object>} broker @throws {Error} missing metadata or route failure
   */
  async partitionBroker(item, brokerId) {
    if (brokerId === undefined) {
      const leaders = this.cluster.findLeaderForPartitions(item.topic, [item.partition])
      const ids = Object.keys(leaders)
      if (ids.length !== 1 || !leaders[ids[0]].includes(item.partition))
        throw new KafkaJSError('Partition leader unavailable', { retriable: true })
      brokerId = Number(ids[0])
    }
    return checked(await waitOf(() => this.cluster.findBroker({ nodeId: brokerId })))
  }

  /** Describes producer state without touching producer instances.
   * @param {object} options topicPartitions and optional brokerId
   * @returns {Promise<object[]>} topic/partition results, int64 values as strings
   * @throws {Error} invalid input, capability, identity or deadline failure
   */
  async describeProducers(options) {
    const { topicPartitions, brokerId } = this.options(options)
    const items = this.partitions(topicPartitions)
    if (brokerId !== undefined) this.integer(brokerId, 2147483647, 'brokerId')
    let refresh = brokerId === undefined
    if (brokerId === undefined) {
      for (const { topic } of this.topics(items))
        checked(await waitOf(() => this.cluster.addTargetTopic(topic)))
    }
    const rows = await this.items(
      items,
      (row) => {
        if (row.errorCode === 6 && brokerId === undefined) {
          refresh = true
          return true
        }
        return row.errorCode === 7
      },
      async (pending) => {
        if (refresh) {
          checked(await waitOf(() => this.cluster.refreshMetadata()))
          refresh = false
        }
        return this.batches(
          pending,
          (item) => this.partitionBroker(item, brokerId),
          async (broker, cells) => {
            const result = checked(
              await waitOf(() => broker.describeProducers({ topics: this.topics(cells) }))
            )
            const names = new Set()
            const expectedTopics = new Set(cells.map((cell) => cell.topic))
            const rows = []
            if (!Array.isArray(result.topics))
              throw new KafkaJSInvariantViolation('Missing producer topics')
            for (const entry of result.topics) {
              if (
                names.has(entry.topic) ||
                !expectedTopics.has(entry.topic) ||
                !Array.isArray(entry.partitions)
              )
                throw new KafkaJSInvariantViolation('Invalid producer topic identity')
              names.add(entry.topic)
              rows.push(...entry.partitions.map((part) => ({ ...part, topic: entry.topic })))
            }
            return this.exact(cells, rows)
          }
        )
      }
    )
    const result = new Map(
      this.topics(items).map(({ topic }) => [topic, { topic, partitions: [] }])
    )
    for (const { topic, ...partition } of rows) result.get(topic).partitions.push(partition)
    return [...result.values()]
  }

  /** Describes transactions, batching only ids with the same coordinator.
   * @param {object} options nonempty unique transactionalIds
   * @returns {Promise<object[]>} per-id results; broker sentinel fields retained on errors
   * @throws {Error} invalid input, capability, identity or deadline failure
   */
  async describeTransactions(options) {
    const { transactionalIds } = this.options(options)
    const items = this.unique(transactionalIds, (id) => this.text(id)).map((transactionalId) => ({
      transactionalId,
      state: 'Unknown',
      timeoutMs: 0,
      startTimeMs: '-1',
      producerId: '-1',
      producerEpoch: -1,
      topics: [],
    }))
    const routes = new Map()
    const unown = currentDeadline()?.own(() => routes.clear())
    try {
      return await this.items(
        items,
        (row) => {
          if (row.errorCode === 15 || row.errorCode === 16) routes.delete(row.transactionalId)
          return [14, 15, 16].includes(row.errorCode)
        },
        (pending) =>
          this.batches(
            pending,
            ({ transactionalId }) => this.coordinator(transactionalId, routes),
            async (broker, cells) =>
              checked(
                await waitOf(() =>
                  broker.describeTransactions({
                    transactionalIds: cells.map((cell) => cell.transactionalId),
                  })
                )
              ).transactions
          )
      )
    } finally {
      unown?.()
      routes.clear()
    }
  }

  /** Lists all brokers sequentially; any broker's final failure rejects the whole call.
   * @param {object} [options={}] optional states, producerIds, durationFilter and broker RE2 string
   * @returns {Promise<object>} transactions and deduplicated unknownStateFilters in encounter order
   * @throws {Error} invalid filter, capability, aggregate broker error or deadline
   */
  async listTransactions(options = {}) {
    const input = this.options(options)
    const states =
      input.states === undefined ? [] : this.unique(input.states, (v) => this.text(v, true), true)
    const producerIds =
      input.producerIds === undefined
        ? []
        : this.unique(input.producerIds, (v) => this.producerId(v), true)
    const durationFilter =
      input.durationFilter === undefined
        ? '-1'
        : String(this.integer(input.durationFilter, Number.MAX_SAFE_INTEGER, 'durationFilter'))
    const transactionalIdPattern =
      input.transactionalIdPattern === undefined
        ? null
        : this.text(input.transactionalIdPattern, true)
    checked(await waitOf(() => this.cluster.refreshMetadata()))
    const nodeIds = Object.keys(this.cluster.brokerPool.brokers)
    const transactions = []
    const unknown = new Set()
    for (const nodeId of nodeIds) {
      const result = await retrierOf(this.retry)(async (bail) => {
        try {
          const broker = checked(await waitOf(() => this.cluster.findBroker({ nodeId })))
          return checked(
            await waitOf(() =>
              broker.listTransactions({
                states,
                producerIds,
                durationFilter,
                transactionalIdPattern,
              })
            )
          )
        } catch (error) {
          checked(null)
          if (error.code === 14 || (error.code === undefined && error.retriable === true))
            throw error
          bail(error)
        }
      })
      transactions.push(...result.transactions)
      for (const value of result.unknownStateFilters) unknown.add(value)
    }
    return { transactions, unknownStateFilters: [...unknown] }
  }

  /** Writes exactly one abort marker; an applied remote mutation cannot be recalled by expiry.
   * @param {object} options topic, partition, producerId, producerEpoch and coordinatorEpoch
   * @returns {Promise<void>} completion @throws {Error} typed terminal protocol/identity/deadline failure
   */
  async abortTransaction(options) {
    const input = this.options(options)
    const topic = this.text(input.topic)
    const partition = this.integer(input.partition, 2147483647, 'partition')
    const producerId = this.producerId(input.producerId)
    const producerEpoch = this.integer(input.producerEpoch, 32767, 'producerEpoch')
    const coordinatorEpoch = this.integer(input.coordinatorEpoch, 2147483647, 'coordinatorEpoch')
    checked(await waitOf(() => this.cluster.addTargetTopic(topic)))
    let refresh = true
    await retrierOf(this.retry)(async (bail) => {
      try {
        if (refresh) {
          checked(await waitOf(() => this.cluster.refreshMetadata()))
          refresh = false
        }
        const broker = checked(await waitOf(() => this.partitionBroker({ topic, partition })))
        const response = checked(
          await waitOf(() =>
            broker.writeTxnMarkers({
              markers: [
                {
                  producerId,
                  producerEpoch,
                  transactionResult: false,
                  topics: [{ topic, partitions: [partition] }],
                  coordinatorEpoch,
                  transactionVersion: 0,
                },
              ],
            })
          )
        )
        if (
          response.markers?.length !== 1 ||
          response.markers[0].producerId !== producerId ||
          response.markers[0].topics?.length !== 1 ||
          response.markers[0].topics[0].topic !== topic ||
          response.markers[0].topics[0].partitions?.length !== 1 ||
          response.markers[0].topics[0].partitions[0].partition !== partition
        )
          throw new KafkaJSInvariantViolation('Invalid abort marker identity')
        const code = response.markers[0].topics[0].partitions[0].errorCode
        if (!Number.isInteger(code))
          throw new KafkaJSInvariantViolation('Invalid abort marker error code')
        if (code !== 0) throw createErrorFromCode(code)
      } catch (error) {
        checked(null)
        if (error.code === 6) {
          refresh = true
          throw new KafkaJSError(error, { retriable: true, cause: error })
        }
        if (error.code === 7 || (error.code === undefined && error.retriable === true)) throw error
        bail(error)
      }
    })
  }

  /** Fences each id through the existing InitProducerId sentinel request.
   * @param {object} options transactionalIds and optional transactionTimeout (default 60000ms)
   * @returns {Promise<object[]>} per-id outcomes, new producerId/epoch or null on unresolved errors
   * @throws {Error} invalid input, capability or deadline; producer state is never reused
   */
  async fenceProducers(options) {
    const input = this.options(options)
    const items = this.unique(input.transactionalIds, (id) => this.text(id)).map(
      (transactionalId) => ({ transactionalId, producerId: null, producerEpoch: null })
    )
    const transactionTimeout =
      input.transactionTimeout === undefined
        ? 60000
        : this.integer(input.transactionTimeout, 2147483647, 'transactionTimeout', 1)
    const routes = new Map()
    const unown = currentDeadline()?.own(() => routes.clear())
    try {
      return await this.items(
        items,
        (row) => {
          if (row.errorCode === 15 || row.errorCode === 16) routes.delete(row.transactionalId)
          return [14, 15, 16, 51].includes(row.errorCode)
        },
        async (pending) => {
          const rows = []
          for (const item of pending) {
            let broker
            try {
              broker = await this.coordinator(item.transactionalId, routes)
            } catch (error) {
              rows.push(this.failed(item, error))
              continue
            }
            try {
              const result = checked(
                await waitOf(() =>
                  broker.initProducerId({
                    transactionalId: item.transactionalId,
                    transactionTimeout,
                  })
                )
              )
              rows.push({
                ...item,
                producerId: result.producerId,
                producerEpoch: result.producerEpoch,
                errorCode: 0,
              })
            } catch (error) {
              const reply =
                error instanceof KafkaJSProtocolError &&
                responseSourceOf(error) === apiKeys.InitProducerId
              rows.push(this.failed(item, error, reply))
            }
          }
          return rows
        }
      )
    } finally {
      unown?.()
      routes.clear()
    }
  }
}

/** Binds the five methods for composition before the public deadline wrapper.
 * @param {object} options cluster and retry @returns {object} public method map
 * @throws {Error} only when dependencies are invalid
 */
module.exports = (options) => {
  const service = new TransactionAdmin(options)
  return Object.fromEntries(
    [
      'describeProducers',
      'describeTransactions',
      'listTransactions',
      'abortTransaction',
      'fenceProducers',
    ].map((name) => [name, service[name].bind(service)])
  )
}
