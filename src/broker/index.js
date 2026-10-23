const Lock = require('../utils/lock')
const describeThrown = require('../network/describeThrown')
const { Types: Compression } = require('../protocol/message/compression')
const { requests, lookup, selectVersion } = require('../protocol/requests')
const { partitionResults } = require('../protocol/requests/produce/partitionResults')
const { NO_TOPIC_ID } = require('../protocol/requests/topicIds')
const { assertSameProtocol } = require('../protocol/requests/groupProtocolCheck')
const {
  KafkaJSConnectionClosedError,
  KafkaJSNonRetriableError,
  KafkaJSServerDoesNotSupportApiKey,
} = require('../errors')
const { attachFailureSource } = require('./failureSource')
const apiKeys = require('../protocol/requests/apiKeys')
const { GROUP_STATES, GROUP_TYPES, toDisplayName } = require('../protocol/groupTypes')
const shuffle = require('../utils/shuffle')

// The last Fetch version keyed by topic name. A request that supplies no topic ids cannot be sent
// above it, whatever the broker advertises.
// @see clients/src/main/resources/common/message/FetchRequest.json (Apache Kafka 4.3.1)
const FETCH_TOPIC_NAMES_MAX_VERSION = 12

// The last OffsetCommit and OffsetFetch version keyed by topic name. A request that carries no
// topic ids cannot be sent above it, whatever the broker advertises.
// @see clients/src/main/resources/common/message/OffsetCommitRequest.json (Apache Kafka 4.3.1)
const OFFSETS_TOPIC_NAMES_MAX_VERSION = 9

/**
 * The topic ids an OffsetCommit or OffsetFetch may be keyed by. All or nothing, as Java's consumer
 * does: one topic without a usable id (or no named topic at all, which is how an OffsetFetch asks
 * for every topic of the group) keeps the whole request keyed by name, because the answer to an
 * all-topics request names topics this client may never have seen.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/CommitRequestManager.java
 * @param {{ topic: string, topicId?: string|null }[]|null} [topics] the request's topic entries
 * @returns {Map<string, string>|undefined} topic name to topic id, or undefined for a request by name
 */
const offsetsTopicIds = (topics) => {
  if (topics == null || topics.length === 0) {
    return undefined
  }

  const topicIds = new Map()
  for (const { topic, topicId } of topics) {
    if (topicId == null || topicId === NO_TOPIC_ID) {
      return undefined
    }

    topicIds.set(topic, topicId)
  }

  return topicIds
}

// The last SyncGroup version that does not carry the group's protocol type and name. From v5 a
// coordinator answers INCONSISTENT_GROUP_PROTOCOL to a request with either one null, so a request
// that names only one of them, or neither, cannot be sent above it.
// @see clients/src/main/java/org/apache/kafka/common/requests/SyncGroupRequest.java (Apache Kafka 4.3.1, areMandatoryProtocolTypeAndNamePresent)
const SYNC_GROUP_NO_PROTOCOL_MAX_VERSION = 4

// The last Produce version keyed by topic name. A request that supplies no topic ids cannot be
// sent above it, whatever the broker advertises.
// @see clients/src/main/resources/common/message/ProduceRequest.json (Apache Kafka 4.3.1)
const PRODUCE_TOPIC_NAMES_MAX_VERSION = 11

// The last Produce, EndTxn and TxnOffsetCommit versions a transaction may use while its
// coordinator runs transaction version 1: the next version of each carries the second part of
// KIP-890 (partitions and offsets added implicitly, the epoch bumped per transaction).
// @see clients/src/main/java/org/apache/kafka/common/requests/EndTxnRequest.java (Apache Kafka 4.3, LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2)
const TRANSACTION_V1_MAX_VERSIONS = {
  [apiKeys.Produce]: 11,
  [apiKeys.EndTxn]: 4,
  [apiKeys.TxnOffsetCommit]: 4,
}

/**
 * The version cap a transactional EndTxn or TxnOffsetCommit takes: the last version before the
 * second part of KIP-890 under transaction version 1, none under 2.
 * @param {number} apiKey
 * @param {number} transactionVersion only 2 means transaction version 2
 * @returns {number|undefined} undefined when nothing caps the request
 */
const transactionVersionCap = (apiKey, transactionVersion) =>
  transactionVersion === 2 ? undefined : TRANSACTION_V1_MAX_VERSIONS[apiKey]

// A transactional Produce under transaction version 2 is sent at exactly this version: v13 keys
// topics by id, while a transaction tracks its partitions by name.
// @see clients/src/main/resources/common/message/ProduceRequest.json (Apache Kafka 4.3.1)
const TRANSACTION_V2_PRODUCE_VERSION = 12

/**
 * The highest Produce version one request may negotiate before the caller's own `maxVersion`.
 * A request without topic ids is keyed by name, so it stays at v11 or below. A transactional one
 * is gated by its transaction version instead, whatever ids it carries: v11 or below under 1,
 * exactly v12 under 2, whose broker adds the partitions to the transaction itself (KIP-890).
 * @param {object} request
 * @param {string|null} [request.transactionalId]
 * @param {number} request.transactionVersion only 2 means transaction version 2
 * @param {Map<string, string>} [request.topicIds]
 * @returns {number|undefined} undefined when nothing caps the request
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
 */
const produceCeiling = ({ transactionalId, transactionVersion, topicIds }) => {
  if (transactionalId != null) {
    return transactionVersion === 2
      ? TRANSACTION_V2_PRODUCE_VERSION
      : TRANSACTION_V1_MAX_VERSIONS[apiKeys.Produce]
  }

  return topicIds == null ? PRODUCE_TOPIC_NAMES_MAX_VERSION : undefined
}

const PRIVATE = {
  SEND_REQUEST: Symbol('private:Broker:sendRequest'),
  API_VERSIONS_AT: Symbol('private:Broker:apiVersionsAt'),
  NEGOTIATE_API_VERSIONS_VERSION: Symbol('private:Broker:negotiateApiVersionsVersion'),
  NEGOTIATE: Symbol('private:Broker:negotiate'),
  CAPPED_LOOKUPS: Symbol('private:Broker:cappedLookups'),
}

/** @type {import("../protocol/requests").Lookup} */
const notInitializedLookup = () => {
  throw new Error('Broker not connected')
}

/**
 * @param {{ apiVersions: { apiKey: number, minVersion: number, maxVersion: number }[] }} response
 * @returns {import("../../types").ApiVersions}
 */
const toVersionMap = (response) =>
  response.apiVersions.reduce(
    (obj, version) =>
      Object.assign(obj, {
        [version.apiKey]: { minVersion: version.minVersion, maxVersion: version.maxVersion },
      }),
    {}
  )

/**
 * The finalized features an ApiVersions response carries from v3 (tagged fields 1 and 2), keyed by
 * feature name. A response below v3, or one without the tags, finalizes nothing: `{}` and -1.
 * The epoch is an int64 the codec decodes as a decimal string.
 * @param {{ finalizedFeatures?: { name: string, minVersionLevel: number, maxVersionLevel: number }[], finalizedFeaturesEpoch?: string }} response
 * @returns {{ finalizedFeatures: { [name: string]: { minVersionLevel: number, maxVersionLevel: number } }, finalizedFeaturesEpoch: number }}
 * @see clients/src/main/resources/common/message/ApiVersionsResponse.json (Apache Kafka 4.3.1)
 */
const toFinalizedFeatures = ({ finalizedFeatures = [], finalizedFeaturesEpoch = '-1' }) => ({
  finalizedFeatures: Object.fromEntries(
    finalizedFeatures.map(({ name, minVersionLevel, maxVersionLevel }) => [
      name,
      { minVersionLevel, maxVersionLevel },
    ])
  ),
  finalizedFeaturesEpoch: Number(finalizedFeaturesEpoch),
})

/**
 * @param {string[]} typeFilters display names
 * @returns {boolean} whether dropping the filter on a broker with no group types yields the same
 *   set of groups: true only for `{Classic}` and for `{Classic, Consumer}`, because on such a
 *   broker every group is classic
 */
const isDroppableTypeFilter = (typeFilters) => {
  const requested = new Set(typeFilters)
  return (
    requested.has(GROUP_TYPES.classic) &&
    [...requested].every((type) => type === GROUP_TYPES.classic || type === GROUP_TYPES.consumer)
  )
}

/**
 * Each node in a Kafka cluster is called broker. This class contains
 * the high-level operations a node can perform.
 *
 * @type {import("../../types").Broker}
 */
module.exports = class Broker {
  /**
   * @param {Object} options
   * @param {import("../network/connectionPool")} options.connectionPool
   * @param {import("../../types").Logger} options.logger
   * @param {number} [options.nodeId]
   * @param {import("../../types").ApiVersions} [options.versions=null] The object with all available versions and APIs
   *                                 supported by this cluster. The output of broker#apiVersions
   * @param {number} [options.authenticationTimeout=10000]
   * @param {boolean} [options.allowAutoTopicCreation=true] If this and the broker config 'auto.create.topics.enable'
   *                                                are true, topics that don't exist will be created when
   *                                                fetching metadata.
   */
  constructor({
    connectionPool,
    logger,
    nodeId = null,
    versions = null,
    authenticationTimeout = 10000,
    allowAutoTopicCreation = true,
  }) {
    this.connectionPool = connectionPool

    /**
     * Whether this broker was replaced (`retire()`). It never goes back: a pool offered later is
     * refused, so the pool attached to a retired broker is itself retired.
     * @type {boolean}
     */
    this.retired = false
    this.nodeId = nodeId
    this.rootLogger = logger
    this.logger = logger.namespace('Broker')
    this.versions = versions

    /**
     * The finalized features of the ApiVersions response `versions` came from (v3+), keyed by
     * feature name. Replaced together with `versions` at every negotiation, so both always
     * describe the same connection generation; `{}` before the first one.
     * @type {{ [name: string]: { minVersionLevel: number, maxVersionLevel: number } }}
     */
    this.finalizedFeatures = {}

    /**
     * The epoch `finalizedFeatures` were finalized at: -1 when the broker sent none, which every
     * response below ApiVersions v3 does. The features are meaningful only when it is >= 0.
     * @type {number}
     */
    this.finalizedFeaturesEpoch = -1

    /**
     * The `connectionPool.generation` the cached `versions` were learned at, or `null` before
     * the first negotiation. Versions handed in by `BrokerPool` are a hint only — KIP-35 rule 7
     * makes them valid per connection — so `connect()` re-negotiates whenever this doesn't match
     * the pool's current generation, and applies the fresh table to both pooled sockets.
     * @type {number|null}
     */
    this.versionsGeneration = null
    this.authenticationTimeout = authenticationTimeout
    this.allowAutoTopicCreation = allowAutoTopicCreation

    // ConnectionPool doesn't otherwise know the client's authenticationTimeout; it needs it
    // to size the post-SASL-failure blackout window (see ConnectionPool#recordAuthenticationFailure).
    this.connectionPool.authenticationTimeout = authenticationTimeout

    // The lock timeout has twice the connectionTimeout because the same timeout is used
    // for the first apiVersions call
    const lockTimeout = 2 * this.connectionPool.connectionTimeout + this.authenticationTimeout
    this.brokerAddress = `${this.connectionPool.host}:${this.connectionPool.port}`

    this.lock = new Lock({
      timeout: lockTimeout,
      description: `connect to broker ${this.brokerAddress}`,
    })

    this.lookupRequest = notInitializedLookup
    this[PRIVATE.CAPPED_LOOKUPS] = new Map()
  }

  /**
   * Swaps in a whole new connection pool (e.g. after `ILLEGAL_SASL_STATE`, which the old pool
   * can't recover from). The replacement is numbered on from the generation of the pool it
   * replaces, so a `(broker, generation)` pair never repeats while the broker lives and a failure
   * of the old pool cannot be mistaken for one of the new. It must be unconnected. Resetting
   * `versionsGeneration` here, rather than leaving that to the caller, guarantees the next
   * `connect()` always re-negotiates against the replacement pool.
   *
   * A retired broker stays retired: the pool offered is retired and closed in the background,
   * having opened nothing, and the broker keeps its own. This does not throw, so a caller that
   * built the pool before the broker was retired (the illegal-SASL recovery, an endpoint hint)
   * finds the broker retired at its next `connect()`, which refuses.
   *
   * @public
   * @param {import("../network/connectionPool")} connectionPool
   * @returns {boolean} whether the pool was installed; false for a retired broker
   */
  replaceConnectionPool(connectionPool) {
    if (this.retired) {
      connectionPool.retire()
      this.closeInBackground(connectionPool, 'Failed to close the pool of a retired broker')
      return false
    }

    connectionPool.continueGenerationFrom(this.connectionPool.generation)
    this.connectionPool = connectionPool
    this.connectionPool.authenticationTimeout = this.authenticationTimeout
    this.versionsGeneration = null
    this[PRIVATE.CAPPED_LOOKUPS].clear()
    return true
  }

  /**
   * A broker is only "connected" once it can actually serve a request: the pool's socket(s) are
   * up, authenticated where required, **and** at least one ApiVersions negotiation has completed
   * so `lookupRequest` is no longer the placeholder that throws. Without the last condition, a
   * caller outside `connect()`'s lock (`BrokerPool#connectBroker`'s own `isConnected()` check,
   * most notably) could see a freshly-connected-but-not-yet-negotiated socket as ready, skip
   * calling `connect()` altogether, and crash the first real request on "Broker not connected".
   * A later reconnect that leaves a *stale but still valid* table in place while re-negotiating
   * does not fail this check — `lookupRequest` is only ever the placeholder before the first
   * negotiation ever completes, never again afterwards.
   *
   * This tightens `isConnected()`'s public meaning: it used to report only socket-level
   * connectivity, so a `Broker` handed to user code (e.g. via `Cluster#findBroker`) could see
   * `true` before this broker was actually able to serve a request. The only direction this can
   * change an answer is false-"connected" becoming a correct `false`; a broker this previously
   * reported connected for is unaffected.
   *
   * It also requires the negotiated table to belong to the *current* connection generation. The
   * pool re-establishes a socket on its own — most importantly the Fetch socket, which is pool
   * index 1 — without going through `connect()`, and a version table learned over a superseded
   * socket is not valid for the new one (KIP-35 rule 7). Reporting `false` sends every existing
   * readiness gate back through `connect()`, which re-negotiates and re-applies the table to both
   * pooled sockets. As with the negotiation term above, the only answer this can change is a
   * false "connected" becoming a correct `false`.
   *
   * @public
   * @returns {boolean}
   */
  isConnected() {
    const connected = this.connectionPool.sasl
      ? this.connectionPool.isConnected() && this.connectionPool.isAuthenticated()
      : this.connectionPool.isConnected()
    return (
      connected &&
      this.lookupRequest !== notInitializedLookup &&
      this.versionsGeneration === this.connectionPool.generation
    )
  }

  /**
   * Establishes the connection pool and negotiates API versions.
   *
   * ApiVersions is re-run whenever this broker's connection pool has been (re)established since
   * the last negotiation — either of its two pooled sockets being new counts (KIP-35 rule 7),
   * since version information is only valid for the connection it was learned on. The resulting
   * table is shared by both pooled sockets. The same negotiation also retains the broker's
   * finalized features (`finalizedFeatures`, `finalizedFeaturesEpoch`).
   *
   * @public
   * @param {object} [options]
   * @param {(frameLength?: number) => Error|null} [options.guard] a producer's private check of its
   *   lifecycle, asked before the lock, the connection, the negotiation and the authentication, and
   *   after each: an error it returns is thrown, so a closed producer goes no further
   * @returns {Promise}
   * @throws {KafkaJSConnectionError} when this broker was retired
   * @throws {Error} the error the guard returned
   */
  async connect({ guard } = {}) {
    this.throwIfRetired()
    this.throwIfRefused(guard)
    await this.lock.acquire()
    try {
      this.throwIfRetired()
      this.throwIfRefused(guard)
      if (this.isConnected()) {
        return
      }

      const connection = await (guard
        ? this.connectionPool.getConnection(0, { guard })
        : this.connectionPool.getConnection())
      this.throwIfRefused(guard)

      // Read once: this is the generation the negotiated table below is learned at, not
      // whatever the pool happens to report after the round trip (a concurrent socket coming
      // up mid-negotiation must not be attributed to this table).
      const generation = this.connectionPool.generation
      if (this.versionsGeneration !== generation) {
        const response = await this[PRIVATE.NEGOTIATE](guard)
        this.throwIfRefused(guard)
        const { finalizedFeatures, finalizedFeaturesEpoch } = toFinalizedFeatures(response)
        this.versions = toVersionMap(response)
        this.finalizedFeatures = finalizedFeatures
        this.finalizedFeaturesEpoch = finalizedFeaturesEpoch
        this.versionsGeneration = generation
      }

      this.throwIfRetired()
      this.connectionPool.setVersions(this.versions)
      this.lookupRequest = lookup(this.versions)
      this[PRIVATE.CAPPED_LOOKUPS].clear()

      this.throwIfRefused(guard)
      await connection.authenticate()
      this.throwIfRetired()
      this.throwIfRefused(guard)
    } finally {
      await this.lock.release()
    }
  }

  /**
   * Closes the connections of this broker. It stays reusable: a later `connect()` works.
   * @public
   * @param {object} [options]
   * @param {number} [options.drainTimeout] ms to wait for the requests of each connection; the
   *   pool's own bound when omitted, which is none unless the cluster set one
   * @returns {Promise}
   */
  async disconnect(options) {
    await this.connectionPool.destroy(options)
  }

  /**
   * Retires a broker that was replaced: its pool opens nothing again, so an operation that held a
   * reference across an await fails with a retriable `KafkaJSConnectionError` (from `connect()`,
   * `getConnection()` or `send()`) and looks the broker up again, and its connections drain for at
   * most `drainTimeout`. A retired broker stays retired.
   * @private
   * @param {object} [options]
   * @param {number} [options.drainTimeout] ms to wait for requests; the pool's own when omitted
   * @returns {Promise} settles when the connections are closed; callers do not wait for it
   */
  retire(options) {
    this.retired = true
    this.connectionPool.retire()
    return this.disconnect(options)
  }

  /**
   * @private
   * @throws {KafkaJSConnectionError} when this broker was retired, whichever pool is attached
   */
  throwIfRetired() {
    if (this.retired) {
      this.connectionPool.retire()
    }

    this.connectionPool.throwIfRetired()
  }

  /**
   * Negotiates the API versions of the current connection (KIP-35, KIP-511).
   *
   * The client asks for the highest ApiVersions version it implements. A broker that does not
   * support it answers UNSUPPORTED_VERSION (35) with a **v0-shaped body** that still carries the
   * `api_keys` entry for ApiVersions itself, so the retry can pick the exact best version in one
   * step. An empty array means a pre-2.4 broker: fall back to v0. INVALID_REQUEST (42) means the
   * broker rejected the client software name or version and is fatal for this connection; it is
   * only handled on the first attempt, since the retried version is always ≤ v2 and sends no
   * client software identity for a broker to reject.
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-511%3A+Collect+and+Expose+Client%27s+Name+and+Version+in+the+Brokers
   *
   * @public
   * @returns {Promise<import("../../types").ApiVersions>}
   */
  async apiVersions() {
    return toVersionMap(await this[PRIVATE.NEGOTIATE]())
  }

  /**
   * The ApiVersions round trip behind `apiVersions()`, resolving the whole decoded response so
   * `connect()` can keep its finalized features as well as its version table.
   * @private
   * @param {(frameLength?: number) => Error|null} [guard] a producer's private pre-write check
   * @returns {Promise<object>} the decoded ApiVersions response
   * @throws {KafkaJSNonRetriableError} when the broker rejects this client's software identity
   */
  async [PRIVATE.NEGOTIATE](guard) {
    const bestImplementedVersion = Math.max(...requests.ApiVersions.versions.map(Number))

    try {
      return await this[PRIVATE.API_VERSIONS_AT](bestImplementedVersion, guard)
    } catch (e) {
      if (e.type === 'INVALID_REQUEST') {
        await this.disconnect()
        throw new KafkaJSNonRetriableError(
          {
            message: `The broker rejected this client's software name or version (INVALID_REQUEST); see KIP-511`,
            type: e.type,
            code: e.code,
          },
          { cause: e }
        )
      }

      if (e.type !== 'UNSUPPORTED_VERSION') {
        throw e
      }

      const retryVersion = this[PRIVATE.NEGOTIATE_API_VERSIONS_VERSION](
        e.apiVersions,
        bestImplementedVersion
      )

      return await this[PRIVATE.API_VERSIONS_AT](retryVersion, guard)
    }
  }

  /**
   * The version `lookup()` would select for one API on this broker's current table, as a number.
   *
   * `lookup()` returns a *builder*, so a caller cannot read the negotiated version without
   * building a request; this answers the question directly. It never throws: a broker that
   * advertises nothing usable answers `null`, which callers read as "this API is not available
   * here" and degrade rather than crash.
   *
   * @internal
   * @param {number} apiKey
   * @param {object} definition the entry from `requests`
   * @param {{ [apiKey: number]: number }} [maxVersions] the same optional ceiling `lookup` takes
   * @returns {number|null} the version, or null when this broker cannot serve the API - including
   *   before it has been connected, when nothing has been negotiated yet
   */
  negotiatedVersion(apiKey, definition, maxVersions) {
    // Null until the first successful connect(), and `selectVersion` would dereference it.
    if (this.versions == null) {
      return null
    }

    try {
      return selectVersion(this.versions, apiKey, definition, maxVersions)
    } catch (e) {
      if (e.name === 'KafkaJSServerDoesNotSupportApiKey') {
        return null
      }

      throw e
    }
  }

  /**
   * `lookupRequest`, with an optional per-request ceiling.
   *
   * A capped builder is memoised per `(apiKey, maxVersion)` because the cap is a hot-path
   * decision — a fetch downgraded to v12 while a topic id is unknown, a produce held at v11 or
   * v12 by its transaction version — and rebuilding the selector per request would allocate on
   * it. The map holds one entry per cap a caller uses, a handful per `Broker` instance. It is
   * cleared wherever `lookupRequest` or `versionsGeneration` is assigned, so it can never outlive
   * the version table it was derived from.
   *
   * @internal
   * @param {number} apiKey
   * @param {object} definition the entry from `requests`
   * @param {number} [maxVersion] the highest version this request may negotiate
   * @returns {import("../protocol/requests").Request}
   * @throws {Error} `Broker not connected` before the first successful `connect()`, same as
   *   `lookupRequest` itself — a cap must not bypass that guard by reaching for `this.versions`
   *   (still `null` at that point) directly.
   * @throws {KafkaJSServerDoesNotSupportApiKey} when no implemented version is at or below the cap
   */
  lookupRequestWithMax(apiKey, definition, maxVersion) {
    if (maxVersion === undefined || this.lookupRequest === notInitializedLookup) {
      return this.lookupRequest(apiKey, definition)
    }

    const key = `${apiKey}:${maxVersion}`
    const memoised = this[PRIVATE.CAPPED_LOOKUPS].get(key)

    if (memoised) {
      return memoised
    }

    const built = lookup(this.versions, { [apiKey]: maxVersion })(apiKey, definition)
    this[PRIVATE.CAPPED_LOOKUPS].set(key, built)
    return built
  }

  /**
   * @private
   * @param {number} version
   * @param {(frameLength?: number) => Error|null} [guard] a producer's private pre-write check
   */
  async [PRIVATE.API_VERSIONS_AT](version, guard) {
    const apiVersions = requests.ApiVersions.protocol({ version })
    return await this[PRIVATE.SEND_REQUEST](
      {
        ...apiVersions(),
        requestTimeout: this.connectionPool.connectionTimeout,
      },
      guard
    )
  }

  /**
   * @private
   * @param {{ apiKey: number, maxVersion: number }[]} [brokerVersions] the api_keys array the
   *        broker returned alongside UNSUPPORTED_VERSION
   * @param {number} bestImplementedVersion
   * @returns {number} the single version to retry with
   */
  [PRIVATE.NEGOTIATE_API_VERSIONS_VERSION](brokerVersions, bestImplementedVersion) {
    const advertised = (brokerVersions || []).find(
      (version) => version.apiKey === apiKeys.ApiVersions
    )

    if (advertised == null) {
      return 0
    }

    const candidate = Math.min(bestImplementedVersion, advertised.maxVersion)

    // A malformed api_keys entry (missing or negative maxVersion) must not turn the retry into
    // `versions[undefined] is not a function`; v0 is always implemented and always parseable.
    return requests.ApiVersions.versions.includes(String(candidate)) ? candidate : 0
  }

  /**
   * @public
   * @type {import("../../types").Broker['metadata']}
   * @param {string[]} [topics=[]] An array of topics to fetch metadata for.
   *                            If no topics are specified fetch metadata for all topics
   * @param {object} [options]
   * @param {(frameLength?: number) => Error|null} [options.guard] a producer's private pre-write
   *   check; it never reaches the request
   */
  async metadata(topics = [], { guard } = {}) {
    this.throwIfRefused(guard)
    const metadata = this.lookupRequest(apiKeys.Metadata, requests.Metadata)
    const shuffledTopics = shuffle(topics)
    return await this[PRIVATE.SEND_REQUEST](
      metadata({ topics: shuffledTopics, allowAutoTopicCreation: this.allowAutoTopicCreation }),
      guard
    )
  }

  /**
   * Describes topics partition by partition, one page at a time (KIP-966). Any broker answers.
   *
   * The broker returns topics in name order, at most `responsePartitionLimit` partitions per page,
   * and `nextCursor` names where the next page starts, or is `null` after the last one. A topic the
   * broker cannot describe comes back as a row with its own `errorCode` and a `null` topic id, not
   * as an error. An empty `topics` list asks for every topic.
   * @public
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas
   * @param {object} request
   * @param {string[]} request.topics the topic names to describe
   * @param {number} [request.responsePartitionLimit=2000] the most partitions one page holds; the
   *   broker clamps it to at least 1 and at most its own limit
   * @param {{ topicName: string, partitionIndex: number }|null} [request.cursor=null] the topic and
   *   partition to resume from; the topic must be in `topics`
   * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number, topics: object[],
   *   nextCursor: { topicName: string, partitionIndex: number }|null }>}
   * @throws {KafkaJSServerDoesNotSupportApiKey} when the broker does not offer the API
   */
  async describeTopicPartitions({ topics, responsePartitionLimit = 2000, cursor = null }) {
    const describeTopicPartitions = this.lookupRequest(
      apiKeys.DescribeTopicPartitions,
      requests.DescribeTopicPartitions
    )
    return await this[PRIVATE.SEND_REQUEST](
      describeTopicPartitions({ topics, responsePartitionLimit, cursor })
    )
  }

  /**
   * @public
   * @param {Object} request
   * @param {Array} request.topicData An array of messages per topic and per partition, example:
   *                          [
   *                            {
   *                              topic: 'test-topic-1',
   *                              partitions: [
   *                                {
   *                                  partition: 0,
   *                                  firstSequence: 0,
   *                                  messages: [
   *                                    { key: '1', value: 'A' },
   *                                    { key: '2', value: 'B' },
   *                                  ]
   *                                },
   *                                {
   *                                  partition: 1,
   *                                  firstSequence: 0,
   *                                  messages: [
   *                                    { key: '3', value: 'C' },
   *                                  ]
   *                                }
   *                              ]
   *                            },
   *                            {
   *                              topic: 'test-topic-2',
   *                              partitions: [
   *                                {
   *                                  partition: 4,
   *                                  firstSequence: 0,
   *                                  messages: [
   *                                    { key: '32', value: 'E' },
   *                                  ]
   *                                },
   *                              ]
   *                            },
   *                          ]
   * @param {number} [request.acks=-1] Control the number of required acks.
   *                           -1 = all replicas must acknowledge
   *                            0 = no acknowledgments
   *                            1 = only waits for the leader to acknowledge
   * @param {number} [request.timeout=30000] The time to await a response in ms
   * @param {string} [request.transactionalId=null]
   * @param {number} [request.producerId=-1] Broker assigned producerId
   * @param {number} [request.producerEpoch=0] Broker assigned producerEpoch
   * @param {import("../../types").CompressionTypes} [request.compression=CompressionTypes.None] Compression codec
   * @param {number} [request.compressionLevel] optional level for the selected codec
   * @param {Map<string, string>} [request.topicIds] topic name to topic id. When every topic in
   *   this request has one, the request can be negotiated at v13, which keys topics by id
   *   (KIP-516); the inverse map is derived here and bound onto the response codec so the decoded
   *   shape - and the `topic` field of a produce error - is unchanged at every version.
   * @param {number} [request.maxVersion] the highest version this request may negotiate
   * @param {1|2} [request.transactionVersion=1] the transaction version in force for a request
   *   carrying a `transactionalId`: 1 keeps it at v11 or below; 2 sends it at v12, whose broker
   *   adds the partitions to the transaction itself (KIP-890). A transactional request never uses
   *   v13. Ignored without a `transactionalId`.
   * @param {(frameLength?: number) => Error|null} [request.guard] private pre-write check;
   *   the complete encoded frame length is supplied only at transmission
   * @param {boolean} [request.perPartitionResults=false] resolve every partition's outcome, a
   *   failing one included, instead of throwing the first failing partition's error. With
   *   `acks: 0` the broker sends no response, so the call resolves `undefined` either way.
   *   Internal: serves the producer's send path; not typed.
   * @returns {Promise<object|import('../protocol/requests/produce/partitionResults').PartitionResult[]|undefined>}
   *   the decoded response, or with `perPartitionResults` one `PartitionResult` per partition;
   *   `undefined` with `acks: 0`
   * @throws {KafkaJSServerDoesNotSupportApiKey} under transaction version 2, when the version the
   *   request would negotiate is below Produce v12 - this broker offers no v12, or `maxVersion` is
   *   under 12 - because a lower version would not add the partition to the transaction
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
   */
  async produce({
    topicData,
    topicIds,
    transactionalId,
    producerId,
    producerEpoch,
    acks = -1,
    timeout = 30000,
    compression = Compression.None,
    compressionLevel,
    maxVersion,
    transactionVersion = 1,
    perPartitionResults = false,
    guard,
  }) {
    this.throwIfRefused(guard)
    // The ceiling is permanent, not scaffolding: `Broker` is a public type and
    // `broker.produce({ topicData })` must keep working forever. A caller may still ask for less.
    const ceiling = produceCeiling({ transactionalId, transactionVersion, topicIds })
    const cap =
      ceiling === undefined
        ? maxVersion
        : Math.min(ceiling, maxVersion === undefined ? Infinity : maxVersion)

    const produce = this.lookupRequestWithMax(apiKeys.Produce, requests.Produce, cap)
    // Asked rather than read off the built request, unlike `fetch`: the id -> name map below has
    // to exist before the builder runs, because the response codec is bound to it.
    // `negotiatedVersion` with no cap map is the uncapped question; passing a sentinel through the
    // map to mean "no cap" would be a workaround for `undefined`.
    const version =
      cap === undefined
        ? this.negotiatedVersion(apiKeys.Produce, requests.Produce)
        : this.negotiatedVersion(apiKeys.Produce, requests.Produce, { [apiKeys.Produce]: cap })

    if (ceiling === TRANSACTION_V2_PRODUCE_VERSION && version < TRANSACTION_V2_PRODUCE_VERSION) {
      const { minVersion, maxVersion: offeredMax } = this.versions[apiKeys.Produce]
      const offers = `Broker ${this.brokerAddress} offers Produce v${minVersion}-v${offeredMax}`
      throw new KafkaJSServerDoesNotSupportApiKey(
        offeredMax < TRANSACTION_V2_PRODUCE_VERSION
          ? `${offers}, but a transaction under transaction version 2 needs v${TRANSACTION_V2_PRODUCE_VERSION}`
          : `${offers}, but maxVersion ${maxVersion} leaves a transaction under transaction version 2 ` +
              `below the v${TRANSACTION_V2_PRODUCE_VERSION} it needs`,
        { apiKey: apiKeys.Produce, apiName: 'Produce' }
      )
    }

    // Built only at v13, the only version that needs it: below that nothing extra is allocated.
    let topicNames
    if (topicIds != null && version >= 13) {
      topicNames = new Map()
      for (const [name, id] of topicIds) {
        topicNames.set(id, name)
      }
    }

    const protocolRequest = produce({
      acks,
      timeout,
      compression,
      compressionLevel,
      topicData,
      topicIds,
      topicNames,
      transactionalId,
      producerId,
      producerEpoch,
    })

    if (!perPartitionResults) {
      return await this[PRIVATE.SEND_REQUEST](protocolRequest, guard)
    }

    return await this[PRIVATE.SEND_REQUEST](
      {
        ...protocolRequest,
        response: { decode: protocolRequest.response.decode, parse: partitionResults },
      },
      guard
    )
  }

  /**
   * @public
   * @param {Object} request
   * @param {number} [request.replicaId=-1] Broker id of the follower. For normal consumers, use -1
   * @param {number} [request.isolationLevel=1] This setting controls the visibility of transactional records. Default READ_COMMITTED.
   * @param {number} [request.maxWaitTime=5000] Maximum time in ms to wait for the response
   * @param {number} [request.minBytes=1] Minimum bytes to accumulate in the response
   * @param {number} [request.maxBytes=10485760] Maximum bytes to accumulate in the response. Note that this is
   *                                   not an absolute maximum, if the first message in the first non-empty
   *                                   partition of the fetch is larger than this value, the message will still
   *                                   be returned to ensure that progress can be made. Default 10MB.
   * @param {Array} request.topics Topics to fetch
   *                        [
   *                          {
   *                            topic: 'topic-name',
   *                            partitions: [
   *                              {
   *                                partition: 0,
   *                                fetchOffset: '4124',
   *                                maxBytes: 2048
   *                              }
   *                            ]
   *                          }
   *                        ]
   * @param {string} [request.rackId=''] A rack identifier for this client. This can be any string value which indicates where this
   *                           client is physically located. It corresponds with the broker config `broker.rack`.
   * @param {boolean} [request.checkCrcs=true] Verify the CRC32C checksum of every record batch in the response
   *                           (Java's `check.crcs`). A mismatch is a corrupt batch (`KafkaJSCorruptRecordError`,
   *                           reason `checksum`) that ends its partition's records in the response.
   * @param {number} [request.maxDecompressedBatchBytes=1073741824] The most a record batch may decompress
   *                           to, in bytes, for every codec. It also sets the record ceiling,
   *                           `max(1, floor(bytes / 512))` records per batch. A batch past either is a corrupt
   *                           batch (`KafkaJSCorruptRecordError`) that ends its partition's records in the response.
   * @param {Map<string, string>} [request.topicIds] topic name -> topic id, for every topic in
   *   this request. A request that supplies none is capped at the last version keyed by name, so a
   *   caller that knows nothing about topic ids keeps working unchanged.
   * @param {number} [request.maxVersion] an additional ceiling; the lower of the two wins.
   * @returns {Promise<object>} `{ responses: [{ topicName, partitions }] }` at every version. An
   *   id this request did not ask for (a broker bug) comes back as `topicName: null` with its
   *   `topicId` intact; deciding what to do about that belongs to the caller, which is why nothing
   *   is logged here.
   */
  async fetch({
    replicaId,
    isolationLevel,
    maxWaitTime = 5000,
    minBytes = 1,
    maxBytes = 10485760,
    topics,
    topicIds,
    maxVersion,
    rackId = '',
    checkCrcs = true,
    maxDecompressedBatchBytes,
  }) {
    // TODO: validate topics not null/empty
    // Ids are what v13+ is keyed by, so a request without them cannot be sent above v12. A caller
    // may still ask for something lower.
    const cap =
      topicIds == null
        ? Math.min(FETCH_TOPIC_NAMES_MAX_VERSION, maxVersion === undefined ? Infinity : maxVersion)
        : maxVersion

    const fetch = this.lookupRequestWithMax(apiKeys.Fetch, requests.Fetch, cap)

    // Shuffle topic-partitions to ensure fair response allocation across partitions (KIP-74)
    const flattenedTopicPartitions = topics.reduce((topicPartitions, { topic, partitions }) => {
      partitions.forEach((partition) => {
        topicPartitions.push({ topic, partition })
      })
      return topicPartitions
    }, [])

    const shuffledTopicPartitions = shuffle(flattenedTopicPartitions)

    // Consecutive partitions for the same topic can be combined into a single `topic` entry
    const consolidatedTopicPartitions = shuffledTopicPartitions.reduce(
      (topicPartitions, { topic, partition }) => {
        const last = topicPartitions[topicPartitions.length - 1]

        if (last != null && last.topic === topic) {
          topicPartitions[topicPartitions.length - 1].partitions.push(partition)
        } else {
          topicPartitions.push({ topic, partitions: [partition] })
        }

        return topicPartitions
      },
      []
    )

    const built = fetch({
      replicaId,
      isolationLevel,
      maxWaitTime,
      minBytes,
      maxBytes,
      topics: consolidatedTopicPartitions,
      topicIds,
      rackId,
      checkCrcs,
      maxDecompressedBatchBytes,
    })

    const response = await this[PRIVATE.SEND_REQUEST](built)

    // `lookupRequest` returns a builder, so the negotiated version is only observable on the
    // request it built. Below v13 the response is already keyed by name and nothing is allocated.
    if (topicIds == null || built.request.apiVersion <= FETCH_TOPIC_NAMES_MAX_VERSION) {
      return response
    }

    // The request's own mapping, never the metadata cache: a topic recreated between request and
    // response would make a cache lookup answer with the new incarnation's pairing.
    const topicNames = new Map()
    for (const [name, id] of topicIds) {
      topicNames.set(id, name)
    }

    for (const entry of response.responses) {
      const name = topicNames.get(entry.topicId)

      if (name === undefined) {
        entry.topicName = null
        continue
      }

      entry.topicName = name
      // Deleted rather than set to undefined: the returned shape is exactly
      // `{ topicName, partitions }` at every version, so the key must not survive at all.
      delete entry.topicId
    }

    return response
  }

  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId The group id
   * @param {number} request.groupGenerationId The generation of the group
   * @param {string} request.memberId The member id assigned by the group coordinator
   * @param {string} [request.groupInstanceId=null] Static membership id (KIP-345); identifies this
   *                                member across restarts. Requires Heartbeat v3+.
   * @returns {Promise}
   */
  async heartbeat({ groupId, groupGenerationId, memberId, groupInstanceId }) {
    const heartbeat = this.lookupRequest(apiKeys.Heartbeat, requests.Heartbeat)
    return await this[PRIVATE.SEND_REQUEST](
      heartbeat({ groupId, groupGenerationId, memberId, groupInstanceId })
    )
  }

  /**
   * One KIP-848 ConsumerGroupHeartbeat: the entire membership protocol for a consumer-protocol
   * member - join, keep-alive, assignment acknowledgement and leave are all this request, told
   * apart by `memberEpoch` and by which fields carry a value.
   *
   * Every optional field means "unchanged since the last heartbeat" when it is left out, so the
   * caller owns the send-if-changed decision and this method invents nothing.
   *
   * @public
   * @param {object} request
   * @param {string} request.groupId
   * @param {string} request.memberId generated by the consumer and kept for its lifetime (KIP-1082)
   * @param {number} request.memberEpoch 0 to join, -1 to leave, -2 for a static member rejoining
   * @param {string|null} [request.instanceId=null] static membership id (KIP-345)
   * @param {string|null} [request.rackId=null]
   * @param {number} [request.rebalanceTimeoutMs=-1]
   * @param {string[]|null} [request.subscribedTopicNames=null] `[]` means "no names", which is
   *   what a regex subscription sends; null means "unchanged"
   * @param {string|null} [request.subscribedTopicRegex=null] requires v1; `''` clears a regex
   * @param {string|null} [request.serverAssignor=null]
   * @param {{topicId: string, partitions: number[]}[]|null} [request.topicPartitions=null]
   * @returns {Promise<object>} the decoded response, including a nullable `assignment`
   * @throws {KafkaJSNonRetriableError} when a regex is given and the broker only offers v0
   * @throws {KafkaJSServerDoesNotSupportApiKey} when the broker does not offer the API
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
   */
  async consumerGroupHeartbeat({
    groupId,
    memberId,
    memberEpoch,
    instanceId = null,
    rackId = null,
    rebalanceTimeoutMs = -1,
    subscribedTopicNames = null,
    subscribedTopicRegex = null,
    serverAssignor = null,
    topicPartitions = null,
  }) {
    // Only a regex needs the version up front, and this method runs once per heartbeat interval:
    // `negotiatedVersion` walks the implemented-version list, so it stays out of the common path.
    // It answers null when the broker offers nothing usable; that case belongs to `lookupRequest`
    // below, which reports it as an unsupported API rather than as a regex problem.
    if (subscribedTopicRegex != null) {
      const version = this.negotiatedVersion(
        apiKeys.ConsumerGroupHeartbeat,
        requests.ConsumerGroupHeartbeat
      )

      if (version !== null && version < 1) {
        throw new KafkaJSNonRetriableError(
          `A regex subscription requires ConsumerGroupHeartbeat v1, and this broker offers v${version}`
        )
      }
    }

    const consumerGroupHeartbeat = this.lookupRequest(
      apiKeys.ConsumerGroupHeartbeat,
      requests.ConsumerGroupHeartbeat
    )

    return await this[PRIVATE.SEND_REQUEST](
      consumerGroupHeartbeat({
        groupId,
        memberId,
        memberEpoch,
        instanceId,
        rackId,
        rebalanceTimeoutMs,
        subscribedTopicNames,
        subscribedTopicRegex,
        serverAssignor,
        topicPartitions,
      })
    )
  }

  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId The unique group id
   * @param {import("../protocol/coordinatorTypes").CoordinatorType} request.coordinatorType The type of coordinator to find
   * @param {(frameLength?: number) => Error|null} [request.guard] a producer's private pre-write
   *   check; it never reaches the request
   * @returns {Promise}
   */
  async findGroupCoordinator({ groupId, coordinatorType, guard }) {
    // TODO: validate groupId, mandatory
    this.throwIfRefused(guard)
    const findCoordinator = this.lookupRequest(apiKeys.GroupCoordinator, requests.GroupCoordinator)
    return await this[PRIVATE.SEND_REQUEST](findCoordinator({ groupId, coordinatorType }), guard)
  }

  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId The unique group id
   * @param {number} request.sessionTimeout The coordinator considers the consumer dead if it receives
   *                                no heartbeat after this timeout in ms
   * @param {number} request.rebalanceTimeout The maximum time that the coordinator will wait for each member
   *                                  to rejoin when rebalancing the group
   * @param {string} [request.memberId=""] The assigned consumer id or an empty string for a new consumer
   * @param {string} [request.groupInstanceId=null] Static membership id (KIP-345); identifies this member
   *                                across restarts so it keeps its partitions instead of rebalancing.
   *                                Requires JoinGroup v5+.
   * @param {string} [request.protocolType="consumer"] Unique name for class of protocols implemented by group
   * @param {Array} request.groupProtocols List of protocols that the member supports (assignment strategy)
   *                                [{ name: 'AssignerName', metadata: '{"version": 1, "topics": []}' }]
   * @returns {Promise}
   * @throws {KafkaJSProtocolError} INCONSISTENT_GROUP_PROTOCOL when the coordinator reports a
   *   protocol type (JoinGroup v7 and later) other than `protocolType`; a null report is accepted
   */
  async joinGroup({
    groupId,
    sessionTimeout,
    rebalanceTimeout,
    memberId = '',
    groupInstanceId = null,
    protocolType = 'consumer',
    groupProtocols,
  }) {
    const joinGroup = this.lookupRequest(apiKeys.JoinGroup, requests.JoinGroup)
    const makeRequest = (assignedMemberId = memberId) =>
      this[PRIVATE.SEND_REQUEST](
        joinGroup({
          groupId,
          sessionTimeout,
          rebalanceTimeout,
          memberId: assignedMemberId,
          groupInstanceId,
          protocolType,
          groupProtocols,
        })
      )

    let response
    try {
      response = await makeRequest()
    } catch (error) {
      if (error.name !== 'KafkaJSMemberIdRequired') {
        throw error
      }

      response = await makeRequest(error.memberId)
    }

    assertSameProtocol(response.protocolType, protocolType)
    return response
  }

  /**
   * Removes members from a group. Two forms:
   *
   * - one member, `{ groupId, memberId, groupInstanceId, reason }`, which throws the first
   *   member's error (the consumer's own leave relies on it), and
   * - several, `{ groupId, members: [{ memberId, groupInstanceId, reason }] }`, which resolves
   *   every member's own code and throws only for a top-level error. A static member can be named
   *   by its instance id alone, with `memberId: ''`.
   *
   * `reason` is sent from LeaveGroup v5 (KIP-800) and dropped below it; the coordinator only logs it.
   * @public
   * @param {object} request
   * @param {string} request.groupId
   * @param {string} [request.memberId]
   * @param {string} [request.groupInstanceId=null] Static membership id (KIP-345). A classic static
   *                                member sends no LeaveGroup on a normal shutdown (see
   *                                `Runner#stop`); this is reached for an explicit leave.
   * @param {string} [request.reason] why the member leaves
   * @param {{ memberId: string, groupInstanceId?: string|null, reason?: string|null }[]} [request.members]
   *   the batch form; when given, the single-member fields are ignored
   * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number, errorCode: number,
   *   members: { memberId: string, groupInstanceId: string|null, errorCode: number }[] }>} the
   *   decoded answer at v3 and later (v0 to v2 answer `{ throttleTime, errorCode }`)
   * @throws {KafkaJSProtocolError} the first member's error (single form), or any top-level error
   */
  async leaveGroup({ groupId, memberId, groupInstanceId, reason, members }) {
    const leaveGroup = this.lookupRequest(apiKeys.LeaveGroup, requests.LeaveGroup)
    return await this[PRIVATE.SEND_REQUEST](
      leaveGroup({ groupId, memberId, groupInstanceId, reason, members })
    )
  }

  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId
   * @param {number} request.generationId
   * @param {string} request.memberId
   * @param {string} [request.groupInstanceId=null] Static membership id (KIP-345); identifies this
   *                                member across restarts. Requires SyncGroup v3+.
   * @param {object} request.groupAssignment
   * @param {string|null} [request.protocolType=null] The group's protocol type, `'consumer'`, as the
   *                                JoinGroup answer reports it. Sent from SyncGroup v5.
   * @param {string|null} [request.protocolName=null] The protocol name the JoinGroup selected. Sent
   *                                from SyncGroup v5.
   * @param {number} [request.sessionTimeout] Used to derive the SyncGroup request timeout
   * @param {number} [request.rebalanceTimeout] Used to derive the SyncGroup request timeout
   * @returns {Promise} The request goes out at v5 only when both `protocolType` and `protocolName`
   *   are given (a coordinator refuses a v5 request with either null); otherwise it is capped at v4.
   * @throws {KafkaJSProtocolError} INCONSISTENT_GROUP_PROTOCOL when the coordinator reports a
   *   protocol type or name other than the one sent; a null report is accepted
   */
  async syncGroup({
    groupId,
    generationId,
    memberId,
    groupInstanceId,
    groupAssignment,
    protocolType,
    protocolName,
    sessionTimeout,
    rebalanceTimeout,
  }) {
    const syncGroup = this.lookupRequestWithMax(
      apiKeys.SyncGroup,
      requests.SyncGroup,
      protocolType != null && protocolName != null ? undefined : SYNC_GROUP_NO_PROTOCOL_MAX_VERSION
    )
    const response = await this[PRIVATE.SEND_REQUEST](
      syncGroup({
        groupId,
        generationId,
        memberId,
        groupInstanceId,
        groupAssignment,
        protocolType,
        protocolName,
        sessionTimeout,
        rebalanceTimeout,
      })
    )

    assertSameProtocol(response.protocolType, protocolType)
    assertSameProtocol(response.protocolName, protocolName)
    return response
  }

  /**
   * @public
   * @param {object} request
   * @param {number} request.replicaId=-1 Broker id of the follower. For normal consumers, use -1
   * @param {number} request.isolationLevel=1 This setting controls the visibility of transactional records (default READ_COMMITTED, Kafka >0.11 only)
   * @param {TopicPartitionOffset[]} request.topics e.g:
   *
   * @typedef {Object} TopicPartitionOffset
   * @property {string} topic
   * @property {PartitionOffset[]} partitions
   *
   * @typedef {Object} PartitionOffset
   * @property {number} partition
   * @property {number} [timestamp=-1]
   *
   *
   * @returns {Promise}
   */
  async listOffsets({ replicaId, isolationLevel, topics }) {
    const listOffsets = this.lookupRequest(apiKeys.ListOffsets, requests.ListOffsets)
    const result = await this[PRIVATE.SEND_REQUEST](
      listOffsets({ replicaId, isolationLevel, topics })
    )

    // ListOffsets >= v1 will return a single `offset` rather than an array of `offsets` (ListOffsets V0).
    // Normalize to just return `offset`.
    for (const response of result.responses) {
      response.partitions = response.partitions.map(({ offsets, ...partitionData }) => {
        return offsets ? { ...partitionData, offset: offsets.pop() } : partitionData
      })
    }

    return result
  }

  /**
   * Asks the leader for the end offset of a leader epoch, so a consumer can tell whether its
   * position still exists on this leader after a leadership change (KIP-320).
   *
   * Per-partition error codes come back in the response rather than as a rejection: the caller
   * classifies them, because a retriable code means "ask again" and a non-retriable one means
   * "stop validating", and neither is an outcome this method can choose.
   *
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation
   * @public
   * @param {object} request
   * @param {{topic: string, partitions: {partition: number, currentLeaderEpoch?: number, leaderEpoch: number}[]}[]} request.topics
   * @returns {Promise<object>} `{ throttleTime, clientSideThrottleTime, topics: [{ topic,
   *   partitions: [{ errorCode, partition, leaderEpoch, endOffset }] }] }`; `endOffset` is a string
   */
  async offsetForLeaderEpoch({ topics }) {
    const offsetForLeaderEpoch = this.lookupRequest(
      apiKeys.OffsetForLeaderEpoch,
      requests.OffsetForLeaderEpoch
    )
    return await this[PRIVATE.SEND_REQUEST](offsetForLeaderEpoch({ topics }))
  }

  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId
   * @param {number} request.groupGenerationId
   * @param {string} request.memberId
   * @param {string} [request.groupInstanceId=null] Static membership id (KIP-345); identifies this
   *                                member across restarts. Requires OffsetCommit v7+.
   * @param {number} [request.retentionTime=-1] -1 signals to the broker that its default configuration
   *                                    should be used.
   * @param {object} request.topics Topics to commit offsets, e.g:
   *                  [
   *                    {
   *                      topic: 'topic-name',
   *                      partitions: [
   *                        { partition: 0, offset: '11', leaderEpoch: 4 }
   *                      ]
   *                    }
   *                  ]
   *                  `leaderEpoch` is optional and requires OffsetCommit v6+: the leader epoch of
   *                  the record whose offset is being committed, or -1 (the default) when no
   *                  record backs the offset. Older versions ignore it.
   *                  Each entry may also carry `topicId`. When every entry does, the request is
   *                  negotiated up to v10 (stable from Kafka 4.2) and keyed by id; otherwise it
   *                  stays at v9 or below, keyed by name. The answer names its topics either way.
   * @param {object} [options] private, for the consumer's own commits
   * @param {(frameLength?: number) => Error|null} [options.guard] a pre-write check. It runs at entry, after the
   *   connection was acquired and connected, after the request was authenticated and encoded, and
   *   at the instant of transmission; an error it returns rejects this one request, which is then
   *   written nowhere, takes no in-flight slot and leaves the connection open.
   * @returns {Promise}
   */
  async offsetCommit(
    { groupId, groupGenerationId, memberId, groupInstanceId, retentionTime, topics },
    options = null
  ) {
    const guard = options?.guard
    this.throwIfRefused(guard)
    const topicIds = offsetsTopicIds(topics)
    const offsetCommit = this.lookupRequestWithMax(
      apiKeys.OffsetCommit,
      requests.OffsetCommit,
      topicIds === undefined ? OFFSETS_TOPIC_NAMES_MAX_VERSION : undefined
    )
    return await this[PRIVATE.SEND_REQUEST](
      offsetCommit({
        groupId,
        groupGenerationId,
        memberId,
        groupInstanceId,
        retentionTime,
        topics,
        topicIds,
      }),
      guard
    )
  }

  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId
   * @param {object} request.topics - If the topic array is null fetch offsets for all topics. e.g:
   *                  [
   *                    {
   *                      topic: 'topic-name',
   *                      partitions: [
   *                        { partition: 0 }
   *                      ]
   *                    }
   *                  ]
   *                  Each entry may also carry `topicId`. When every entry does, the request is
   *                  negotiated up to v10 (stable from Kafka 4.2) and keyed by id; otherwise, and
   *                  for a null or empty list, it stays at v9 or below, keyed by name.
   * @param {string|null} [request.memberId=null] the consumer-protocol member id; the coordinator
   *                  validates it together with the epoch from OffsetFetch v9 upward. A classic
   *                  member and an admin client pass nothing, which sends the field's own "no
   *                  membership" defaults; every version below 9 drops both fields.
   * @param {number} [request.memberEpoch=-1] the consumer-protocol member epoch
   * @param {boolean} [request.requireStable=false] ask the coordinator to hold back the offset of a
   *   partition that has a transactional offset commit pending, answering it with the retriable
   *   UNSTABLE_OFFSET_COMMIT instead (KIP-447, v7+; dropped below v7). The consumer asks for it;
   *   the admin client keeps the default.
   * @returns {Promise}
   */
  async offsetFetch({ groupId, topics, memberId = null, memberEpoch = -1, requireStable = false }) {
    const topicIds = offsetsTopicIds(topics)
    const offsetFetch = this.lookupRequestWithMax(
      apiKeys.OffsetFetch,
      requests.OffsetFetch,
      topicIds === undefined ? OFFSETS_TOPIC_NAMES_MAX_VERSION : undefined
    )
    return await this[PRIVATE.SEND_REQUEST](
      offsetFetch({ groupId, topics, topicIds, memberId, memberEpoch, requireStable })
    )
  }

  /**
   * Describes classic groups. A KIP-848 consumer-protocol group is not describable here at any
   * version: below v6 a broker reports it as `Dead`, and from v6 it answers `GROUP_ID_NOT_FOUND`
   * for a group of another type as it does for a missing one (KIP-1043) - which is what makes
   * such a group recognisable, so the caller can describe it through ConsumerGroupDescribe.
   *
   * From v6 a group that does not exist comes back as a row carrying `GROUP_ID_NOT_FOUND` rather
   * than as a `Dead` row with no error; every other group-level error still fails the whole call.
   *
   * @public
   * @param {object} request
   * @param {string[]} request.groupIds
   * @param {boolean} [request.includeAuthorizedOperations=false] requires v3+; ignored below it
   * @returns {Promise<object>} the decoded response
   * @throws {KafkaJSProtocolError} on any group-level error code other than `GROUP_ID_NOT_FOUND`
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1043%3A+Administration+of+groups
   */
  async describeGroups({ groupIds, includeAuthorizedOperations = false }) {
    const describeGroups = this.lookupRequest(apiKeys.DescribeGroups, requests.DescribeGroups)
    return await this[PRIVATE.SEND_REQUEST](
      describeGroups({ groupIds, includeAuthorizedOperations })
    )
  }

  /**
   * Describes consumer-protocol (KIP-848) groups. The classic `describeGroups` cannot: below
   * DescribeGroups v6 a broker reports such a group as `Dead`.
   *
   * Group-level error codes are RETURNED, not thrown: `GROUP_ID_NOT_FOUND` here means "this is
   * not a consumer-protocol group" and is the caller's signal to ask `describeGroups` for that
   * id instead.
   *
   * @public
   * @param {object} request
   * @param {string[]} request.groupIds
   * @param {boolean} [request.includeAuthorizedOperations=false]
   * @returns {Promise<object>} the decoded response, one entry per requested group
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/admin/internals/DescribeConsumerGroupsHandler.java
   */
  async consumerGroupDescribe({ groupIds, includeAuthorizedOperations = false }) {
    const consumerGroupDescribe = this.lookupRequest(
      apiKeys.ConsumerGroupDescribe,
      requests.ConsumerGroupDescribe
    )
    return await this[PRIVATE.SEND_REQUEST](
      consumerGroupDescribe({ groupIds, includeAuthorizedOperations })
    )
  }

  /**
   * @public
   * @param {object} request
   * @param {Array} request.topics e.g:
   *                 [
   *                   {
   *                     topic: 'topic-name',
   *                     numPartitions: 1,
   *                     replicationFactor: 1
   *                   }
   *                 ]
   * @param {boolean} [request.validateOnly=false] If this is true, the request will be validated, but the topic
   *                                       won't be created
   * @param {number} [request.timeout=5000] The time in ms to wait for a topic to be completely created
   *                                on the controller node
   * @returns {Promise}
   */
  async createTopics({ topics, validateOnly = false, timeout = 5000 }) {
    const createTopics = this.lookupRequest(apiKeys.CreateTopics, requests.CreateTopics)
    return await this[PRIVATE.SEND_REQUEST](createTopics({ topics, validateOnly, timeout }))
  }

  /**
   * @public
   * @param {object} request
   * @param {Array} request.topicPartitions e.g:
   *                 [
   *                   {
   *                     topic: 'topic-name',
   *                     count: 3,
   *                     assignments: []
   *                   }
   *                 ]
   * @param {boolean} [request.validateOnly=false] If this is true, the request will be validated, but the topic
   *                                       won't be created
   * @param {number} [request.timeout=5000] The time in ms to wait for a topic to be completely created
   *                                on the controller node
   * @returns {Promise<void>}
   */
  async createPartitions({ topicPartitions, validateOnly = false, timeout = 5000 }) {
    const createPartitions = this.lookupRequest(apiKeys.CreatePartitions, requests.CreatePartitions)
    return await this[PRIVATE.SEND_REQUEST](
      createPartitions({ topicPartitions, validateOnly, timeout })
    )
  }

  /**
   * @public
   * @param {object} request
   * @param {string[]} request.topics An array of topics to be deleted
   * @param {number} [request.timeout=5000] The time in ms to wait for a topic to be completely deleted on the
   *                                controller node.
   * @returns {Promise}
   */
  async deleteTopics({ topics, timeout = 5000 }) {
    const deleteTopics = this.lookupRequest(apiKeys.DeleteTopics, requests.DeleteTopics)
    return await this[PRIVATE.SEND_REQUEST](deleteTopics({ topics, timeout }))
  }

  /**
   * @public
   * @param {object} request
   * @param {import("../../types").ResourceConfigQuery[]} request.resources
   *                                 [{
   *                                   type: RESOURCE_TYPES.TOPIC,
   *                                   name: 'topic-name',
   *                                   configNames: ['compression.type', 'retention.ms']
   *                                 }]
   * @param {boolean} [request.includeSynonyms=false]
   * @param {boolean} [request.includeDocumentation=false] ask for each entry's documentation; sent
   *                                 from DescribeConfigs v3, ignored by older versions
   * @returns {Promise}
   */
  async describeConfigs({ resources, includeSynonyms = false, includeDocumentation = false }) {
    const describeConfigs = this.lookupRequest(apiKeys.DescribeConfigs, requests.DescribeConfigs)
    return await this[PRIVATE.SEND_REQUEST](
      describeConfigs({ resources, includeSynonyms, includeDocumentation })
    )
  }

  /**
   * @public
   * @param {object} request
   * @param {import("../../types").IResourceConfig[]} request.resources
   *                                 [{
   *                                  type: RESOURCE_TYPES.TOPIC,
   *                                  name: 'topic-name',
   *                                  configEntries: [
   *                                    {
   *                                      name: 'cleanup.policy',
   *                                      value: 'compact'
   *                                    }
   *                                  ]
   *                                 }]
   * @param {boolean} [request.validateOnly=false]
   * @returns {Promise}
   */
  async alterConfigs({ resources, validateOnly = false }) {
    const alterConfigs = this.lookupRequest(apiKeys.AlterConfigs, requests.AlterConfigs)
    return await this[PRIVATE.SEND_REQUEST](alterConfigs({ resources, validateOnly }))
  }

  /**
   * Change individual config entries (KIP-339). Each entry names an operation, so the entries a
   * request leaves out stay as they are, unlike `alterConfigs`, which replaces the whole config.
   * The response carries one result per resource, and a failed resource is a result, not an error.
   * @public
   * @param {object} request
   * @param {{ type: number, name: string,
   *   configEntries: { name: string, operation: number, value?: string|null }[] }[]} request.resources
   *   `operation` is an `AlterConfigOpTypes` value; a DELETE ignores `value`
   * @param {boolean} [request.validateOnly=false]
   * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number, resources:
   *   { errorCode: number, errorMessage: string|null, resourceType: number,
   *   resourceName: string }[] }>}
   */
  async incrementalAlterConfigs({ resources, validateOnly = false }) {
    const incrementalAlterConfigs = this.lookupRequest(
      apiKeys.IncrementalAlterConfigs,
      requests.IncrementalAlterConfigs
    )
    return await this[PRIVATE.SEND_REQUEST](incrementalAlterConfigs({ resources, validateOnly }))
  }

  /**
   * Send an `InitProducerId` request to fetch a PID and bump the producer epoch.
   *
   * Request should be made to the transaction coordinator.
   * @public
   * @param {object} request
   * @param {number} request.transactionTimeout The time in ms to wait for before aborting idle transactions
   * @param {number} [request.transactionalId] The transactional id or null if the producer is not transactional
   * @param {string|number} [request.producerId=-1] the producer id held, on a KIP-360
   *   re-initialization; -1 on a first initialization. Sent from v3, dropped below it
   * @param {number} [request.producerEpoch=-1] the epoch held, on a re-initialization; -1 on a
   *   first initialization
   * @param {(frameLength?: number) => Error|null} [request.guard] a producer's private pre-write
   *   check; it never reaches the request
   * @returns {Promise}
   * @see https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89068820
   */
  async initProducerId({
    transactionalId,
    transactionTimeout,
    producerId = -1,
    producerEpoch = -1,
    guard,
  }) {
    this.throwIfRefused(guard)
    const initProducerId = this.lookupRequest(apiKeys.InitProducerId, requests.InitProducerId)
    return await this[PRIVATE.SEND_REQUEST](
      initProducerId({ transactionalId, transactionTimeout, producerId, producerEpoch }),
      guard
    )
  }

  /**
   * Send an `AddPartitionsToTxn` request to mark a TopicPartition as participating in the transaction.
   *
   * Request should be made to the transaction coordinator.
   * @public
   * @param {object} request
   * @param {string} request.transactionalId The transactional id corresponding to the transaction.
   * @param {number} request.producerId Current producer id in use by the transactional id.
   * @param {number} request.producerEpoch Current epoch associated with the producer id.
   * @param {object[]} request.topics e.g:
   *                  [
   *                    {
   *                      topic: 'topic-name',
   *                      partitions: [ 0, 1]
   *                    }
   *                  ]
   * @param {(frameLength?: number) => Error|null} [request.guard] a producer's private pre-write
   *   check; it never reaches the request
   * @returns {Promise}
   */
  async addPartitionsToTxn({ transactionalId, producerId, producerEpoch, topics, guard }) {
    this.throwIfRefused(guard)
    const addPartitionsToTxn = this.lookupRequest(
      apiKeys.AddPartitionsToTxn,
      requests.AddPartitionsToTxn
    )
    return await this[PRIVATE.SEND_REQUEST](
      addPartitionsToTxn({ transactionalId, producerId, producerEpoch, topics }),
      guard
    )
  }

  /**
   * Send an `AddOffsetsToTxn` request.
   *
   * Request should be made to the transaction coordinator.
   * @public
   * @param {object} request
   * @param {string} request.transactionalId The transactional id corresponding to the transaction.
   * @param {number} request.producerId Current producer id in use by the transactional id.
   * @param {number} request.producerEpoch Current epoch associated with the producer id.
   * @param {string} request.groupId The unique group identifier (for the consumer group)
   * @param {(frameLength?: number) => Error|null} [request.guard] a producer's private pre-write
   *   check; it never reaches the request
   * @returns {Promise}
   */
  async addOffsetsToTxn({ transactionalId, producerId, producerEpoch, groupId, guard }) {
    this.throwIfRefused(guard)
    const addOffsetsToTxn = this.lookupRequest(apiKeys.AddOffsetsToTxn, requests.AddOffsetsToTxn)
    return await this[PRIVATE.SEND_REQUEST](
      addOffsetsToTxn({ transactionalId, producerId, producerEpoch, groupId }),
      guard
    )
  }

  /**
   * Send a `TxnOffsetCommit` request to persist the offsets in the `__consumer_offsets` topics.
   *
   * Request should be made to the consumer coordinator.
   * @public
   * @param {object} request
   * @param {string} request.transactionalId The transactional id corresponding to the transaction.
   * @param {string} request.groupId The unique group identifier (for the consumer group)
   * @param {number} request.producerId Current producer id in use by the transactional id.
   * @param {number} request.producerEpoch Current epoch associated with the producer id.
   * @param {OffsetCommitTopic[]} request.topics
   * @param {number} [request.generationId=-1] the consumer's generation - its member epoch under
   *   the consumer group protocol - so the group coordinator can fence a stale member (KIP-447,
   *   v3+); -1 commits without group fencing
   * @param {string} [request.memberId=''] the consumer's member id; '' commits without group fencing
   * @param {string|null} [request.groupInstanceId=null] the consumer's static instance id
   * @param {1|2} [request.transactionVersion=1] the transaction version in force for this
   *   transaction: 1 keeps the request at v4 or below; 2 allows v5, which also adds the offsets to
   *   the transaction (KIP-890)
   * @param {(frameLength?: number) => Error|null} [request.guard] a producer's private pre-write
   *   check; it never reaches the request
   *
   * @typedef {Object} OffsetCommitTopic
   * @property {string} topic
   * @property {OffsetCommitTopicPartition[]} partitions
   *
   * @typedef {Object} OffsetCommitTopicPartition
   * @property {number} partition
   * @property {number} offset
   * @property {number} [leaderEpoch] the leader epoch of the consumed record, sent as the
   *   partition's `CommittedLeaderEpoch` (v2+); -1 when omitted
   * @property {string} [metadata]
   *
   * @returns {Promise}
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
   */
  async txnOffsetCommit({
    transactionalId,
    groupId,
    producerId,
    producerEpoch,
    topics,
    generationId = -1,
    memberId = '',
    groupInstanceId = null,
    transactionVersion = 1,
    guard,
  }) {
    this.throwIfRefused(guard)
    const txnOffsetCommit = this.lookupRequestWithMax(
      apiKeys.TxnOffsetCommit,
      requests.TxnOffsetCommit,
      transactionVersionCap(apiKeys.TxnOffsetCommit, transactionVersion)
    )
    return await this[PRIVATE.SEND_REQUEST](
      txnOffsetCommit({
        transactionalId,
        groupId,
        producerId,
        producerEpoch,
        topics,
        generationId,
        memberId,
        groupInstanceId,
      }),
      guard
    )
  }

  /**
   * Send an `EndTxn` request to indicate transaction should be committed or aborted.
   *
   * Request should be made to the transaction coordinator.
   * @public
   * @param {object} request
   * @param {string} request.transactionalId The transactional id corresponding to the transaction.
   * @param {number} request.producerId Current producer id in use by the transactional id.
   * @param {number} request.producerEpoch Current epoch associated with the producer id.
   * @param {boolean} request.transactionResult The result of the transaction (false = ABORT, true = COMMIT)
   * @param {1|2} [request.transactionVersion=1] the transaction version in force for this
   *   transaction: 1 keeps the request at v4 or below; 2 allows v5, whose response carries the
   *   producer id and epoch the coordinator bumped to (KIP-890)
   * @param {(frameLength?: number) => Error|null} [request.guard] a producer's private pre-write
   *   check; it never reaches the request
   * @returns {Promise}
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
   */
  async endTxn({
    transactionalId,
    producerId,
    producerEpoch,
    transactionResult,
    transactionVersion = 1,
    guard,
  }) {
    this.throwIfRefused(guard)
    const endTxn = this.lookupRequestWithMax(
      apiKeys.EndTxn,
      requests.EndTxn,
      transactionVersionCap(apiKeys.EndTxn, transactionVersion)
    )
    return await this[PRIVATE.SEND_REQUEST](
      endTxn({ transactionalId, producerId, producerEpoch, transactionResult }),
      guard
    )
  }

  /**
   * Lists the groups this broker coordinates, optionally filtered by state and by type.
   *
   * Both filters are given in lower case (`'stable'`, `'consumer'`) and are mapped to the display
   * names the wire carries. What happens when the broker is too old to understand a filter is
   * Java's rule, verbatim, because the alternatives are all wrong in one direction or the other:
   *
   * - a state filter needs v4; below that it throws, since answering unfiltered would be a
   *   different question;
   * - a type filter needs v5, EXCEPT when it asks for exactly `classic`, or for `classic` and
   *   `consumer`, on a broker with no group types at all - there every group is classic, so
   *   dropping the filter yields the same set. Any other filter throws, `consumer` alone
   *   included: dropping that one would answer "all groups" to "consumer groups only".
   *
   * @public
   * @param {object} [filters]
   * @param {string[]} [filters.states=[]] lower-case group states
   * @param {string[]} [filters.types=[]] lower-case group types
   * @returns {Promise<object>} the decoded response; rows carry `groupState`/`groupType` as the
   *   broker's display strings, or `''` on a version that does not have them
   * @throws {KafkaJSServerDoesNotSupportApiKey} when a filter needs a version this broker does
   *   not offer and cannot be dropped without changing the answer
   * @throws {KafkaJSNonRetriableError} when a filter value is not one Kafka defines
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/requests/ListGroupsRequest.java
   */
  async listGroups({ states = [], types = [] } = {}) {
    const version = this.negotiatedVersion(apiKeys.ListGroups, requests.ListGroups)
    const stateFilters = states.map((state) => toDisplayName(GROUP_STATES, state, 'group state'))
    let typeFilters = types.map((type) => toDisplayName(GROUP_TYPES, type, 'group type'))

    // A null version means the broker advertises nothing usable for this API. That is
    // `lookupRequest`'s error to report, not a filter downgrade, so the two checks below are
    // skipped rather than misreporting "we need v4" for a broker that offers no ListGroups at all.
    if (version !== null && stateFilters.length > 0 && version < 4) {
      throw new KafkaJSServerDoesNotSupportApiKey(
        `The broker only supports ListGroups v${version}, but we need v4 or newer to request groups by states.`,
        { apiKey: apiKeys.ListGroups, apiName: 'ListGroups' }
      )
    }

    if (version !== null && typeFilters.length > 0 && version < 5) {
      if (isDroppableTypeFilter(typeFilters)) {
        typeFilters = []
      } else {
        throw new KafkaJSServerDoesNotSupportApiKey(
          `The broker only supports ListGroups v${version}, but we need v5 or newer to request groups by type. ` +
            `Requested group types: [${typeFilters.join(', ')}].`,
          { apiKey: apiKeys.ListGroups, apiName: 'ListGroups' }
        )
      }
    }

    const listGroups = this.lookupRequest(apiKeys.ListGroups, requests.ListGroups)
    return await this[PRIVATE.SEND_REQUEST](
      listGroups({ states: stateFilters, types: typeFilters })
    )
  }

  /**
   * Send request to delete groups
   * @param {string[]} groupIds
   * @public
   * @returns {Promise}
   */
  async deleteGroups(groupIds) {
    const deleteGroups = this.lookupRequest(apiKeys.DeleteGroups, requests.DeleteGroups)
    return await this[PRIVATE.SEND_REQUEST](deleteGroups(groupIds))
  }

  /**
   * Send request to delete records
   * @public
   * @param {object} request
   * @param {TopicPartitionRecords[]} request.topics
   *                          [
   *                            {
   *                              topic: 'my-topic-name',
   *                              partitions: [
   *                                { partition: 0, offset 2 },
   *                                { partition: 1, offset 4 },
   *                              ],
   *                            }
   *                          ]
   * @returns {Promise<Array>} example:
   *                          {
   *                            throttleTime: 0
   *                           [
   *                              {
   *                                topic: 'my-topic-name',
   *                                partitions: [
   *                                 { partition: 0, lowWatermark: '2n', errorCode: 0 },
   *                                 { partition: 1, lowWatermark: '4n', errorCode: 0 },
   *                               ],
   *                             },
   *                           ]
   *                          }
   *
   * @typedef {object} TopicPartitionRecords
   * @property {string} topic
   * @property {PartitionRecord[]} partitions
   *
   * @typedef {object} PartitionRecord
   * @property {number} partition
   * @property {number} offset
   */
  async deleteRecords({ topics }) {
    const deleteRecords = this.lookupRequest(apiKeys.DeleteRecords, requests.DeleteRecords)
    return await this[PRIVATE.SEND_REQUEST](deleteRecords({ topics }))
  }

  /**
   * @public
   * @param {object} request
   * @param {import("../../types").AclEntry[]} request.acl e.g:
   *                 [
   *                   {
   *                     resourceType: AclResourceTypes.TOPIC,
   *                     resourceName: 'topic-name',
   *                     resourcePatternType: ResourcePatternTypes.LITERAL,
   *                     principal: 'User:bob',
   *                     host: '*',
   *                     operation: AclOperationTypes.ALL,
   *                     permissionType: AclPermissionTypes.DENY,
   *                   }
   *                 ]
   * @returns {Promise<void>}
   */
  async createAcls({ acl }) {
    const createAcls = this.lookupRequest(apiKeys.CreateAcls, requests.CreateAcls)
    return await this[PRIVATE.SEND_REQUEST](createAcls({ creations: acl }))
  }

  /**
   * @public
   * @param {import("../../types").AclEntry} aclEntry
   * @returns {Promise<void>}
   */
  async describeAcls({
    resourceType,
    resourceName,
    resourcePatternType,
    principal,
    host,
    operation,
    permissionType,
  }) {
    const describeAcls = this.lookupRequest(apiKeys.DescribeAcls, requests.DescribeAcls)
    return await this[PRIVATE.SEND_REQUEST](
      describeAcls({
        resourceType,
        resourceName,
        resourcePatternType,
        principal,
        host,
        operation,
        permissionType,
      })
    )
  }

  /**
   * @public
   * @param {Object} request
   * @param {import("../../types").AclEntry[]} request.filters
   * @returns {Promise<void>}
   */
  async deleteAcls({ filters }) {
    const deleteAcls = this.lookupRequest(apiKeys.DeleteAcls, requests.DeleteAcls)
    return await this[PRIVATE.SEND_REQUEST](deleteAcls({ filters }))
  }

  /**
   * @public
   * @param {Object} request
   * @param {import("../../types").PartitionReassignment[]} request.topics
   * @param {number} [request.timeout]
   * @returns {Promise}
   */
  async alterPartitionReassignments({ topics, timeout }) {
    const alterPartitionReassignments = this.lookupRequest(
      apiKeys.AlterPartitionReassignments,
      requests.AlterPartitionReassignments
    )
    return await this[PRIVATE.SEND_REQUEST](alterPartitionReassignments({ topics, timeout }))
  }

  /**
   * @public
   * @param {Object} request
   * @param {import("../../types").TopicPartitions[]} request.topics can be null
   * @param {number} [request.timeout]
   * @returns {Promise}
   */
  async listPartitionReassignments({ topics = null, timeout }) {
    const listPartitionReassignments = this.lookupRequest(
      apiKeys.ListPartitionReassignments,
      requests.ListPartitionReassignments
    )
    return await this[PRIVATE.SEND_REQUEST](listPartitionReassignments({ topics, timeout }))
  }

  /**
   * Dispatches one built request over the pool.
   *
   * The connection is obtained **before** dispatching so a socket the pool has just
   * re-established is visible here: if its generation no longer matches the table this broker
   * negotiated, `connect()` runs first. The check is skipped for `ApiVersions`, which *is* the
   * negotiation request and would otherwise re-enter `connect()`'s non-reentrant lock.
   *
   * The selected connection is acquired again if negotiation let it close. Only then is the
   * generation captured and the request handed to the pool's established-send path, which has no
   * await or reconnect before Connection#send. ApiVersions uses the same acquisition and snapshot
   * without re-entering the negotiation lock. A late reconnect leaves the negotiated table stale
   * so the next request negotiates again; this already-built request keeps its chosen version.
   *
   * The request is bound to the pool it started on: every acquisition and the send go through that
   * one pool, and after each await the pool is checked to still be the broker's. If it was
   * replaced meanwhile, the request is refused before any write with a retriable
   * `KafkaJSConnectionClosedError`, so a connection of the old pool is never sent through the
   * new one, and its failure is attributed to the old pool.
   *
   * It never re-versions a request that has already been built: the caller chose its version
   * before the socket was re-established, and the honest outcome for the rare case where that
   * version is no longer supported is the broker's own `UNSUPPORTED_VERSION`, exactly as before.
   *
   * A failure notes on the error which broker sent the request and the pool generation it went out
   * on (`attachFailureSource`), so the code that drops a failed broker from a table can tell a
   * stale failure from a current one. When the failure is a connection closed under the request,
   * the pool's other connection is closed in the background (`retireSibling`).
   *
   * @private
   * @param {{ request: { apiKey: number } }} protocolRequest a built request, as produced by
   *                                          `lookup()`/`lookupRequestWithMax()`'s returned builder
   * @param {((frameLength?: number) => Error|null)|undefined} guard a pre-write check, see `offsetCommit`
   * @returns {Promise} the broker's parsed response for `protocolRequest`
   * @throws {Error} the error of the failed request, or the one the guard returned
   */
  async [PRIVATE.SEND_REQUEST](protocolRequest, guard) {
    const pool = this.connectionPool
    let generation = pool.generation
    try {
      const connection = await (guard
        ? pool.getConnectionByRequest(protocolRequest, { guard })
        : pool.getConnectionByRequest(protocolRequest))
      this.throwIfReplaced(pool)

      if (
        protocolRequest.request.apiKey !== apiKeys.ApiVersions &&
        this.versionsGeneration !== pool.generation
      ) {
        await (guard ? this.connect({ guard }) : this.connect())
        this.throwIfReplaced(pool)
      }

      if (!connection.isConnected()) {
        await (guard
          ? pool.getConnectionByRequest(protocolRequest, { guard })
          : pool.getConnectionByRequest(protocolRequest))
        this.throwIfReplaced(pool)
      }

      this.throwIfRefused(guard)
      generation = pool.generation
      return await pool.send(protocolRequest, connection, guard ? { guard } : null, true)
    } catch (e) {
      attachFailureSource(e, { broker: this, generation })
      this.retireSibling(e, generation, pool)

      throw e
    }
  }

  /**
   * Refuses a request whose pool was replaced while it waited, before anything is written: the
   * connection it holds belongs to the old pool, which is not where the broker sends any more.
   * @private
   * @param {import("../network/connectionPool")} pool the pool the request started on
   * @throws {KafkaJSConnectionClosedError} (retriable) when `pool` is no longer the broker's pool;
   *   the caller's retrier sends the request again on the current one
   */
  throwIfReplaced(pool) {
    if (this.connectionPool !== pool) {
      throw new KafkaJSConnectionClosedError(
        "The broker's connection pool was replaced before the request was sent",
        { host: pool.host, port: pool.port }
      )
    }
  }

  /**
   * @private
   * @param {((frameLength?: number) => Error|null)|undefined} guard
   * @throws {Error} the error the guard returns
   */
  throwIfRefused(guard) {
    const refusal = guard && guard()
    if (refusal) {
      throw refusal
    }
  }

  /**
   * After a request failed because its connection was closed under it, closes the other
   * connection of the pool gracefully, in the background: no new work, requests in flight finish
   * up to the pool's `drainTimeout`, then the socket closes. It acts only on the pool the request
   * was sent on, and only if no connection of that pool was established since (a reconnect that
   * already happened, or a pool that replaced it, is not touched), and nothing waits for it, so
   * the failing request is rejected at once and never waits behind a healthy long Fetch.
   * @private
   * @param {*} error what the request failed with, which need not be an Error
   * @param {number} generation the pool's generation when the request was sent
   * @param {import("../network/connectionPool")} [pool] the pool the request was sent on; the
   *   current one when omitted
   */
  retireSibling(error, generation, pool = this.connectionPool) {
    if (
      describeThrown(error, 'name') !== 'KafkaJSConnectionClosedError' ||
      this.connectionPool !== pool ||
      pool.generation !== generation
    ) {
      return
    }

    this.closeInBackground(pool, 'Failed to close the sibling connection')
  }

  /**
   * Closes a pool without holding the caller back; a failed close is logged and never surfaces.
   * @private
   * @param {import("../network/connectionPool")} pool
   * @param {string} message what the log entry says when the close fails
   */
  closeInBackground(pool, message) {
    pool.destroy().catch((e) => {
      try {
        this.logger.error(message, { error: describeThrown(e), cause: e })
      } catch {}
    })
  }
}
