const path = require('node:path')
const { KafkaJSInvalidConfigurationError, KafkaJSInvariantViolation } = require('../errors')
const { createErrorFromCode } = require('../protocol/error')
const { itemRetry } = require('./itemRetry')
const { checked, waitOf, currentDeadline } = require('./deadline')

/** Refuses invalid new input before any discovery or state publication.
 *
 * @param {boolean} valid validation predicate
 *
 * @param {string} field stable field name, never caller contents
 *
 * @returns {void}
 *
 * @throws {KafkaJSInvalidConfigurationError} invalid input
 */
const requireInput = (valid, field) => {
  if (!valid) throw new KafkaJSInvalidConfigurationError(`Invalid ${field}`)
}
const record = (x) => x !== null && typeof x === 'object' && !Array.isArray(x)
const name = (x) => typeof x === 'string' && x.length > 0 && !x.includes('\0')
const id = (x) => Number.isInteger(x) && x >= 0 && x <= 2147483647
const keyOf = (x) => JSON.stringify([x.brokerId, x.topic, x.partition])
const resultOf = (row) => ({
  ...row,
  error: row.errorCode === 0 ? null : createErrorFromCode(row.errorCode),
  errorMessage: row.errorMessage ?? null,
})

/** Validates a partition filter, preserving null versus empty.
 *
 * @param {object[]|null|undefined} topics topic and numeric partition selections
 *
 * @returns {object[]|null} copied filter
 *
 * @throws {KafkaJSInvalidConfigurationError} invalid or duplicate identity
 */
const selection = (topics) => {
  if (topics == null) return null
  requireInput(Array.isArray(topics), 'topicPartitions')
  const seen = new Set()
  return topics.map((t) => {
    requireInput(
      record(t) && name(t.topic) && !seen.has(t.topic) && Array.isArray(t.partitions),
      'topicPartitions'
    )
    seen.add(t.topic)
    requireInput(
      t.partitions.every(id) && new Set(t.partitions).size === t.partitions.length,
      'partitions'
    )
    return { topic: t.topic, partitions: t.partitions.slice() }
  })
}

/** Validates replica identities and optional absolute destination paths.
 *
 * @param {object} options request object
 *
 * @param {boolean} moving whether destination paths are required
 *
 * @returns {object[]} detached replica cells
 *
 * @throws {KafkaJSInvalidConfigurationError} invalid input or repeated identity
 */
const replicasOf = (options, moving) => {
  requireInput(
    record(options) && Array.isArray(options.replicas) && options.replicas.length > 0,
    'replicas'
  )
  const seen = new Set()
  return options.replicas.map((r) => {
    requireInput(record(r) && name(r.topic) && id(r.partition) && id(r.brokerId), 'replicas')
    requireInput(!seen.has(keyOf(r)), 'replicas')
    seen.add(keyOf(r))
    if (moving) requireInput(name(r.logDir) && path.posix.isAbsolute(r.logDir), 'logDir')
    return moving
      ? { topic: r.topic, partition: r.partition, brokerId: r.brokerId, logDir: r.logDir }
      : { topic: r.topic, partition: r.partition, brokerId: r.brokerId }
  })
}

/** Locates connection-wide capability/authentication refusals in an inherited retry wrapper.
 *
 * @param {Error} error original or wrapped error
 *
 * @returns {Error|null} original typed refusal
 */
const refusal = (error) => {
  const seen = new Set()
  for (let e = error; e && !seen.has(e); e = e.cause) {
    seen.add(e)
    if (
      [
        'KafkaJSServerDoesNotSupportApiKey',
        'KafkaJSSASLAuthenticationError',
        'KafkaJSInvalidConfigurationError',
      ].includes(e.name)
    )
      return e
  }
  return null
}

/** Checks directory and topic identities and requested partitions; absent replicas remain valid.
 * Partition entries may repeat while the broker swaps current and future logs.
 * @param {object} row broker response
 * @param {object[]|null} selected explicit filter or all
 * @returns {void}
 * @throws {KafkaJSInvariantViolation} invalid, unrequested or duplicate directory/topic identity
 */
const validateAnswer = (row, selected) => {
  const expected =
    selected === null ? null : new Map(selected.map((t) => [t.topic, new Set(t.partitions)]))
  const paths = new Set()
  for (const dir of row.logDirs) {
    if (!name(dir.path) || paths.has(dir.path))
      throw new KafkaJSInvariantViolation('Invalid directory identity')
    paths.add(dir.path)
    const topics = new Set()
    for (const t of dir.topics) {
      if (!name(t.topic) || topics.has(t.topic) || (expected && !expected.has(t.topic)))
        throw new KafkaJSInvariantViolation('Invalid directory topic identity')
      topics.add(t.topic)
      for (const p of t.partitions) {
        if (!id(p.partition) || (expected && !expected.get(t.topic).has(p.partition)))
          throw new KafkaJSInvariantViolation('Invalid directory partition identity')
      }
    }
  }
}

/** Stateless broker-local storage operations under the single enclosing admin deadline. */
class LogDirectoryAdmin {
  /**
   * @param {object} options shared cluster and unchanged retry settings
   *
   * @returns {LogDirectoryAdmin} family with no operation state
   */
  constructor({ cluster, retry }) {
    this.cluster = cluster
    this.retry = retry
  }

  /** Describes requested brokers once each per round; completed brokers are never resent.
   *
   * @param {object} [options] brokers and topicPartitions filters
   *
   * @returns {Promise<object[]>} caller-ordered broker and directory outcomes
   *
   * @throws {Error} validation, deadline or capability/auth refusal regardless of completed peers
   */
  async describeLogDirs(options = {}) {
    return this.describeSelected(options)
  }

  /** Sends the per-broker filters shared by the two directory read methods.
   *
   * @param {object} options public broker and topic filters
   *
   * @param {Map<number,Map<string,number[]>>} [filters] private replica selections
   *
   * @returns {Promise<object[]>} ordered broker outcomes
   *
   * @throws {Error} validation, deadline or capability/auth refusal
   */
  async describeSelected(options = {}, filters) {
    requireInput(record(options), 'options')
    const topics = selection(options.topicPartitions)
    if (options.brokers !== undefined) {
      requireInput(
        Array.isArray(options.brokers) &&
          options.brokers.every(id) &&
          new Set(options.brokers).size === options.brokers.length,
        'brokers'
      )
    }
    if (options.brokers?.length === 0 || topics?.every((t) => t.partitions.length === 0)) return []
    checked(await waitOf(() => this.cluster.refreshMetadata()))
    const brokers =
      options.brokers?.slice() ?? this.cluster.brokerPool.metadata.brokers.map((b) => b.nodeId)
    const results = await Promise.allSettled(
      brokers.map((brokerId) =>
        itemRetry(
          {
            items: [{ brokerId }],
            keyOf: (x) => String(x.brokerId),
            retry: this.retry,
            retryable: (row) => createErrorFromCode(row.errorCode).retriable === true,
            callFailure: (error) => refusal(error) !== null,
            sanitize: (x) => x,
          },
          async () => {
            const broker = checked(
              await waitOf(() => this.cluster.findBroker({ nodeId: brokerId }))
            )
            const selected = filters
              ? [...filters.get(brokerId)].map(([topic, partitions]) => ({ topic, partitions }))
              : topics
            const row = checked(await waitOf(() => broker.describeLogDirs({ topics: selected })))
            validateAnswer(row, selected)
            return [{ brokerId, ...row }]
          }
        )
      )
    )
    return results.map((out, i) => {
      if (out.status === 'rejected') {
        currentDeadline()?.check()
        if (refusal(out.reason)) throw refusal(out.reason)
        if (out.reason instanceof KafkaJSInvariantViolation) throw out.reason
        return {
          brokerId: brokers[i],
          errorCode: -1,
          error: out.reason,
          errorMessage: out.reason.message,
          logDirs: [],
        }
      }
      const row = out.value[0]
      return { ...row, logDirs: (row.logDirs ?? []).map(resultOf) }
    })
  }

  /** Moves replica logs on their named brokers, retaining per-replica errors and ordering.
   *
   * @param {object} options replicas with absolute logDir paths
   *
   * @returns {Promise<object[]>} per-replica AdminItemResult cells
   *
   * @throws {Error} validation, deadline or capability/auth refusal
   */
  async alterReplicaLogDirs(options) {
    const replicas = replicasOf(options, true)
    const groups = new Map()
    checked(await waitOf(() => this.cluster.refreshMetadata()))
    for (const r of replicas) {
      if (!groups.has(r.brokerId)) groups.set(r.brokerId, [])
      groups.get(r.brokerId).push(r)
    }
    const outcomes = await Promise.allSettled(
      [...groups].map(([brokerId, items]) =>
        itemRetry(
          {
            items,
            keyOf,
            retry: this.retry,
            retryable: (row) => row.errorCode === 7,
            callFailure: (error) => refusal(error) !== null,
            sanitize: (x) => x,
          },
          async (pending) => {
            const broker = checked(
              await waitOf(() => this.cluster.findBroker({ nodeId: brokerId }))
            )
            const dirs = new Map()
            for (const r of pending) {
              if (!dirs.has(r.logDir)) dirs.set(r.logDir, new Map())
              const topics = dirs.get(r.logDir)
              if (!topics.has(r.topic)) topics.set(r.topic, [])
              topics.get(r.topic).push(r.partition)
            }
            const response = checked(
              await waitOf(() =>
                broker.alterReplicaLogDirs({
                  dirs: [...dirs].map(([path, topics]) => ({
                    path,
                    topics: [...topics].map(([topic, partitions]) => ({ topic, partitions })),
                  })),
                })
              )
            )
            return response.topics.flatMap((t) =>
              t.partitions.map((p) => ({ brokerId, topic: t.topic, ...p }))
            )
          }
        )
      )
    )
    const all = new Map()
    const entries = [...groups]
    outcomes.forEach((o, i) => {
      if (o.status === 'rejected') {
        currentDeadline()?.check()
        if (refusal(o.reason)) throw refusal(o.reason)
        if (o.reason instanceof KafkaJSInvariantViolation) throw o.reason
        for (const r of entries[i][1])
          all.set(keyOf(r), {
            ...r,
            errorCode: -1,
            error: o.reason,
            errorMessage: o.reason.message,
          })
      } else for (const r of o.value) all.set(keyOf(r), r)
    })
    return replicas.map((r) => {
      const { logDir, ...row } = all.get(keyOf(r))
      return row
    })
  }

  /** Projects one directory read per broker into current/future replica locations.
   * Like Java, keeps the last partition entry per directory before projecting its location.
   * Repeated current/future locations replace that location and lag in response order.
   *
   * @param {object} options unique replicas to locate
   *
   * @returns {Promise<object[]>} caller-ordered locations and source errors; absent replicas have null locations
   *
   * @throws {Error} validation, deadline, malformed directory/topic identity or capability/auth refusal
   */
  async describeReplicaLogDirs(options) {
    const replicas = replicasOf(options, false)
    const groups = new Map()
    for (const r of replicas) {
      if (!groups.has(r.brokerId)) groups.set(r.brokerId, new Map())
      const topics = groups.get(r.brokerId)
      if (!topics.has(r.topic)) topics.set(r.topic, [])
      topics.get(r.topic).push(r.partition)
    }
    const found = await this.describeSelected({ brokers: [...groups.keys()] }, groups)
    const index = new Map()
    for (const broker of found) {
      const cells = new Map()
      const source = broker.logDirs.find((dir) => dir.errorCode !== 0)
      for (const dir of broker.logDirs)
        for (const t of dir.topics)
          for (const p of new Map(t.partitions.map((p) => [p.partition, p])).values()) {
            const key = JSON.stringify([t.topic, p.partition])
            const location = p.isFuture ? 'future' : 'current'
            if (!cells.has(key))
              cells.set(key, {
                currentLogDir: null,
                futureLogDir: null,
                currentOffsetLag: null,
                futureOffsetLag: null,
              })
            const cell = cells.get(key)
            cell[`${location}LogDir`] = dir.path
            cell[`${location}OffsetLag`] = p.offsetLag
          }
      index.set(broker.brokerId, {
        cells,
        error: broker.errorCode !== 0 ? broker : (source ?? broker),
      })
    }
    return replicas.map((r) => {
      const { cells, error } = index.get(r.brokerId)
      return {
        ...r,
        ...(cells.get(JSON.stringify([r.topic, r.partition])) ?? {
          currentLogDir: null,
          futureLogDir: null,
          currentOffsetLag: null,
          futureOffsetLag: null,
        }),
        errorCode: error.errorCode,
        error: error.error,
        errorMessage: error.errorMessage,
      }
    })
  }
}
/** Creates storage methods; public wrapping and the invocation token stay with the admin factory.
 *
 * @param {object} options shared cluster and retry settings
 *
 * @returns {object} three bound public methods
 */
module.exports = (options) => {
  const family = new LogDirectoryAdmin(options)
  return {
    describeLogDirs: family.describeLogDirs.bind(family),
    alterReplicaLogDirs: family.alterReplicaLogDirs.bind(family),
    describeReplicaLogDirs: family.describeReplicaLogDirs.bind(family),
  }
}
