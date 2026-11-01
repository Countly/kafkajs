const { retrierOf, checked, waitOf } = require('./deadline')
const { itemRetry } = require('./itemRetry')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSSASLAuthenticationError,
  KafkaJSNotImplemented,
  KafkaJSServerDoesNotSupportApiKey,
} = require('../errors')
const apiKeys = require('../protocol/requests/apiKeys')
const { requests } = require('../protocol/requests')

/** Cluster operations share the invocation deadline and retain successful item outcomes. */
class ClusterAdmin {
  /** @param {object} context cluster and unchanged retry options @returns {ClusterAdmin} call facade */
  constructor({ cluster, retry }) {
    this.cluster = cluster
    this.retry = retry
  }

  /** Retries aggregate calls without replacing terminal typed errors. @param {Function} work lazy attempt @returns {Promise<any>} result @throws {Error} original terminal or retry exhaustion */
  retryCall(work) {
    return retrierOf(this.retry)(async (bail) => {
      try {
        return checked(await waitOf(work))
      } catch (error) {
        if (!error.retriable) bail(error)
        throw error
      }
    })
  }

  /** Checks a plain option record before discovery. @param {object} options input @returns {void} @throws {KafkaJSInvalidConfigurationError} invalid record */
  options(options) {
    if (!options || typeof options !== 'object' || Array.isArray(options))
      throw new KafkaJSInvalidConfigurationError('Expected an options object')
  }

  /** Checks optional flags without coercion. @param {object} options input @param {string[]} names flags @returns {void} @throws {KafkaJSInvalidConfigurationError} nonboolean flag */
  flags(options, names) {
    this.options(options)
    for (const name of names)
      if (options[name] !== undefined && typeof options[name] !== 'boolean')
        throw new KafkaJSInvalidConfigurationError(`${name} must be boolean`)
  }

  /** Validates unique topic and partition identities. Null means all; an empty array means no work. @param {object[]|null} topics selection @returns {object[]|null} copied flat cells @throws {KafkaJSInvalidConfigurationError} malformed or duplicate input */
  partitions(topics) {
    if (topics === null) return null
    if (!Array.isArray(topics))
      throw new KafkaJSInvalidConfigurationError('topicPartitions must be an array or null')
    const names = new Set()
    const cells = []
    for (const t of topics) {
      if (
        !t ||
        typeof t.topic !== 'string' ||
        !t.topic.trim() ||
        t.topic.includes('\0') ||
        names.has(t.topic) ||
        !Array.isArray(t.partitions) ||
        !t.partitions.length
      )
        throw new KafkaJSInvalidConfigurationError('Invalid topic partition selection')
      names.add(t.topic)
      const seen = new Set()
      for (const p of t.partitions) {
        if (!Number.isInteger(p) || p < 0 || p > 2147483647 || seen.has(p))
          throw new KafkaJSInvalidConfigurationError('Invalid or duplicate partition')
        seen.add(p)
        cells.push({ topic: t.topic, partition: p })
      }
    }
    return cells
  }

  /** Groups flat cells in encounter order. @param {object[]} rows topic/partition cells @returns {object[]} topic lists */
  group(rows) {
    const groups = new Map()
    for (const row of rows) {
      if (!groups.has(row.topic)) groups.set(row.topic, [])
      groups.get(row.topic).push(row)
    }
    return [...groups].map(([topic, partitions]) => ({ topic, partitions }))
  }

  /** Reads and checks controller results before accepting any cell. @param {object} response decoded body @returns {object[]} flat rows @throws {KafkaJSInvariantViolation} duplicate or malformed identities */
  electionRows(response) {
    if (!Array.isArray(response.replicaElectionResults))
      throw new KafkaJSInvariantViolation('Missing election results')
    const names = new Set()
    const keys = new Set()
    const rows = []
    for (const t of response.replicaElectionResults) {
      if (typeof t.topic !== 'string' || names.has(t.topic) || !Array.isArray(t.partitionResult))
        throw new KafkaJSInvariantViolation('Invalid election topic identity')
      names.add(t.topic)
      for (const p of t.partitionResult) {
        const key = JSON.stringify([t.topic, p.partitionId])
        if (
          !Number.isInteger(p.partitionId) ||
          p.partitionId < 0 ||
          keys.has(key) ||
          !Number.isInteger(p.errorCode)
        )
          throw new KafkaJSInvariantViolation('Invalid election partition identity')
        keys.add(key)
        rows.push({
          topic: t.topic,
          partition: p.partitionId,
          errorCode: p.errorCode,
          errorMessage: p.errorMessage ?? null,
        })
      }
    }
    return rows
  }

  /** Sends to the current controller. Code41 refreshes the route before the existing retrier reattempts. @param {string} method Broker operation @param {object} options wire fields @returns {Promise<object>} decoded response @throws {Error} capability, call-level or transport failure */
  async controller(method, options) {
    try {
      checked(await waitOf(() => this.cluster.refreshMetadataIfNecessary()))
      const broker = checked(await waitOf(() => this.cluster.findControllerBroker()))
      return checked(await waitOf(() => broker[method](options)))
    } catch (error) {
      if (error.code === 41) checked(await waitOf(() => this.cluster.refreshMetadata()))
      throw error
    }
  }

  /** Classifies call-level failures that must not be retried; item codes and retriable protocol errors (such as a moved controller) never enter this path. @param {Error} error original error @returns {boolean} whether whole call must reject */
  callFailure(error) {
    return (
      error instanceof KafkaJSInvalidConfigurationError ||
      error instanceof KafkaJSSASLAuthenticationError ||
      error instanceof KafkaJSNotImplemented ||
      error instanceof KafkaJSServerDoesNotSupportApiKey ||
      (error.name === 'KafkaJSProtocolError' && !error.retriable)
    )
  }

  /** Runs item attempts through the shared retrier. A retriable protocol error that outlasts the retries rejects the call instead of leaving every item unresolved. @param {object} config item retry configuration @param {(pending: object[]) => Promise<object[]>} send one attempt for the pending items @returns {Promise<object[]>} caller-ordered item outcomes @throws {Error} deadline, call-level or exhausted protocol failure */
  async retryItems(config, send) {
    let refusal = null
    const rows = await itemRetry(config, async (pending) => {
      try {
        const result = await send(pending)
        refusal = null
        return result
      } catch (error) {
        refusal = error.name === 'KafkaJSProtocolError' ? error : null
        throw error
      }
    })
    if (refusal) throw refusal
    return rows
  }

  /** Elects requested partitions, or all once before narrowing retries. Successful mutations are never resent. Item rows return before relocation; a pending refresh runs only before another unresolved attempt under the inherited deadline. @param {object} [options] election type and topicPartitions @returns {Promise<object[]>} caller-ordered item outcomes @throws {Error} invalid input, deadline, capability or top-level failure */
  async electLeaders(options = {}) {
    this.options(options)
    const { electionType = 'preferred', topicPartitions = null } = options
    if (!['preferred', 'unclean'].includes(electionType))
      throw new KafkaJSInvalidConfigurationError('Invalid electionType')
    let items = this.partitions(topicPartitions)
    if (items !== null && items.length === 0) return []
    let refreshPending = false
    let first = null
    if (items === null) {
      const response = checked(
        await this.retryCall(() =>
          this.controller('electLeaders', { electionType, topicPartitions: null })
        )
      )
      first = this.electionRows(response)
      items = first.map(({ topic, partition }) => ({ topic, partition }))
    }
    const rows = checked(
      await this.retryItems(
        {
          items,
          keyOf: (r) => JSON.stringify([r.topic, r.partition]),
          retry: this.retry,
          retryable: (r) => r.errorCode === 7 || r.errorCode === 41,
          callFailure: (e) => this.callFailure(e),
          sanitize: (m) => m,
        },
        async (pending) => {
          if (refreshPending) {
            checked(await waitOf(() => this.cluster.refreshMetadata()))
            refreshPending = false
          }
          let rows = first
          first = null
          if (rows === null) {
            const selected = this.group(pending).map((t) => ({
              topic: t.topic,
              partitions: t.partitions.map((p) => p.partition),
            }))
            rows = this.electionRows(
              checked(
                await this.controller('electLeaders', { electionType, topicPartitions: selected })
              )
            )
          }
          refreshPending = rows.some((r) => r.errorCode === 41)
          return rows
        }
      )
    )
    return this.group(rows).map((t) => ({
      topic: t.topic,
      partitions: t.partitions.map(({ topic, ...p }) => p),
    }))
  }

  /** Updates unique features. A v2 success covers every requested feature atomically; v0/v1 must carry exact results. Rows return before relocation; failed lazy refreshes retain the requirement and affect only unresolved features under the inherited deadline. @param {object} options feature updates and validateOnly @returns {Promise<object[]>} caller-ordered feature outcomes @throws {Error} invalid input, deadline or call-level failure */
  async updateFeatures(options) {
    this.flags(options, ['validateOnly'])
    if (!Array.isArray(options.features) || !options.features.length)
      throw new KafkaJSInvalidConfigurationError('features must be nonempty')
    const names = new Set()
    const items = options.features.map((f) => {
      if (
        !f ||
        typeof f.name !== 'string' ||
        !f.name.trim() ||
        f.name.includes('\0') ||
        names.has(f.name) ||
        !Number.isInteger(f.maxVersionLevel) ||
        f.maxVersionLevel < 0 ||
        f.maxVersionLevel > 32767 ||
        !['upgrade', 'safe-downgrade', 'unsafe-downgrade'].includes(f.upgradeType ?? 'upgrade') ||
        f.upgradeType === null
      )
        throw new KafkaJSInvalidConfigurationError('Invalid feature update')
      names.add(f.name)
      return {
        feature: f.name,
        maxVersionLevel: f.maxVersionLevel,
        upgradeType: f.upgradeType ?? 'upgrade',
      }
    })
    let refreshPending = false
    const rows = checked(
      await this.retryItems(
        {
          items,
          keyOf: (r) => r.feature,
          retry: this.retry,
          retryable: (r) => r.errorCode === 7 || r.errorCode === 41,
          callFailure: (e) => this.callFailure(e),
          sanitize: (m) => m,
        },
        async (pending) => {
          if (refreshPending) {
            checked(await waitOf(() => this.cluster.refreshMetadata()))
            refreshPending = false
          }
          const response = checked(
            await this.controller('updateFeatures', {
              featureUpdates: pending,
              validateOnly: options.validateOnly ?? false,
            })
          )
          const rows =
            response.results === null
              ? pending.map((f) => ({ feature: f.feature, errorCode: 0, errorMessage: null }))
              : response.results
          refreshPending = Array.isArray(rows) && rows.some((r) => r.errorCode === 41)
          return rows
        }
      )
    )
    return rows.map(({ feature, errorCode, error, errorMessage }) => ({
      feature,
      errorCode,
      error,
      errorMessage,
    }))
  }

  /** Sends a fresh ApiVersions on a random connected broker or the named node. Snapshots used by connect are untouched. @param {object} [options] brokerId @returns {Promise<object>} feature maps and lossless epoch @throws {Error} invalid id, deadline, broker or transport failure */
  async describeFeatures(options = {}) {
    this.options(options)
    if (
      options.brokerId !== undefined &&
      (!Number.isInteger(options.brokerId) || options.brokerId < 0 || options.brokerId > 2147483647)
    )
      throw new KafkaJSInvalidConfigurationError('Invalid brokerId')
    return this.retryCall(async () => {
      const broker = checked(
        await waitOf(() =>
          options.brokerId === undefined
            ? this.cluster.findConnectedBroker()
            : this.cluster.findBroker({ nodeId: options.brokerId })
        )
      )
      const r = checked(await waitOf(() => broker.apiVersions({ includeFeatures: true })))
      const supportedFeatures = Object.fromEntries(
        (r.supportedFeatures ?? []).map(({ name, minVersion, maxVersion }) => [
          name,
          { minVersion, maxVersion },
        ])
      )
      const finalizedFeatures = Object.fromEntries(
        (r.finalizedFeatures ?? []).map(({ name, minVersionLevel, maxVersionLevel }) => [
          name,
          { minVersionLevel, maxVersionLevel },
        ])
      )
      const epoch = String(r.finalizedFeaturesEpoch ?? '-1')
      return {
        supportedFeatures,
        finalizedFeatures,
        finalizedFeaturesEpoch: epoch === '-1' ? null : epoch,
      }
    })
  }

  /** Preserves the legacy Metadata result projection. @param {object} response Metadata body @param {boolean} mask requested raw authorization mask @returns {object} compatible cluster result */
  metadataResult(response, mask = false) {
    return {
      brokers: response.brokers.map(({ nodeId, host, port }) => ({ nodeId, host, port })),
      controller:
        response.controllerId == null || response.controllerId === -1
          ? null
          : response.controllerId,
      clusterId: response.clusterId,
      ...(mask
        ? {
            authorizedOperations:
              response.clusterAuthorizedOperations == null ||
              response.clusterAuthorizedOperations === -2147483648
                ? null
                : response.clusterAuthorizedOperations,
          }
        : {}),
    }
  }

  /** Describes broker endpoints. Default uses exactly the inherited Metadata call; flags opt into key60. @param {object} [options] includeAuthorizedOperations/includeFencedBrokers @returns {Promise<object>} compatible cluster result @throws {Error} invalid flag, capability, deadline or broker failure */
  async describeCluster(options = {}) {
    this.flags(options, ['includeAuthorizedOperations', 'includeFencedBrokers'])
    const mask = options.includeAuthorizedOperations === true
    const fenced = options.includeFencedBrokers === true
    if (!mask && !fenced)
      return this.metadataResult(checked(await waitOf(() => this.cluster.metadata({ topics: [] }))))
    return this.retryCall(async () => {
      const broker = checked(await waitOf(() => this.cluster.findConnectedBroker()))
      if (
        !fenced &&
        broker.negotiatedVersion(apiKeys.DescribeCluster, requests.DescribeCluster) === null
      ) {
        return this.metadataResult(
          checked(
            await waitOf(() => broker.metadata([], { includeClusterAuthorizedOperations: true }))
          ),
          mask
        )
      }
      const r = checked(
        await waitOf(() =>
          broker.describeCluster({
            includeClusterAuthorizedOperations: mask,
            includeFencedBrokers: fenced,
          })
        )
      )
      return {
        brokers: r.brokers.map(({ brokerId, host, port, isFenced }) => ({
          nodeId: brokerId,
          host,
          port,
          ...(fenced ? { fenced: isFenced } : {}),
        })),
        controller: r.controllerId === -1 ? null : r.controllerId,
        clusterId: r.clusterId,
        ...(mask
          ? {
              authorizedOperations:
                r.clusterAuthorizedOperations === -2147483648
                  ? null
                  : r.clusterAuthorizedOperations,
            }
          : {}),
      }
    })
  }
}

/** Creates bound public methods before the shared deadline wrapper. @param {object} context cluster/retry @returns {object} Admin methods */
module.exports = (context) => {
  const owner = new ClusterAdmin(context)
  return Object.fromEntries(
    ['electLeaders', 'updateFeatures', 'describeFeatures', 'describeCluster'].map((name) => [
      name,
      owner[name].bind(owner),
    ])
  )
}
