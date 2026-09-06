const Lock = require('../utils/lock')
const { Types: Compression } = require('../protocol/message/compression')
const { requests, lookup, selectVersion } = require('../protocol/requests')
const { KafkaJSNonRetriableError } = require('../errors')
const apiKeys = require('../protocol/requests/apiKeys')
const shuffle = require('../utils/shuffle')

// The last Fetch version keyed by topic name. A request that supplies no topic ids cannot be sent
// above it, whatever the broker advertises.
// @see clients/src/main/resources/common/message/FetchRequest.json (Apache Kafka 4.3.1)
const FETCH_TOPIC_NAMES_MAX_VERSION = 12

const PRIVATE = {
  SEND_REQUEST: Symbol('private:Broker:sendRequest'),
  API_VERSIONS_AT: Symbol('private:Broker:apiVersionsAt'),
  NEGOTIATE_API_VERSIONS_VERSION: Symbol('private:Broker:negotiateApiVersionsVersion'),
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
    this.nodeId = nodeId
    this.rootLogger = logger
    this.logger = logger.namespace('Broker')
    this.versions = versions

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
   * can't recover from). The new pool restarts its generation count at 0, which can coincide
   * with the value `versionsGeneration` already holds — resetting it here, rather than leaving
   * that to the caller, guarantees the next `connect()` always re-negotiates against the
   * replacement pool instead of comparing across two unrelated pools' generations.
   *
   * @public
   * @param {import("../network/connectionPool")} connectionPool
   */
  replaceConnectionPool(connectionPool) {
    this.connectionPool = connectionPool
    this.connectionPool.authenticationTimeout = this.authenticationTimeout
    this.versionsGeneration = null
    this[PRIVATE.CAPPED_LOOKUPS].clear()
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
   * @public
   * @returns {boolean}
   */
  isConnected() {
    const connected = this.connectionPool.sasl
      ? this.connectionPool.isConnected() && this.connectionPool.isAuthenticated()
      : this.connectionPool.isConnected()
    return connected && this.lookupRequest !== notInitializedLookup
  }

  /**
   * Establishes the connection pool and negotiates API versions.
   *
   * ApiVersions is re-run whenever this broker's connection pool has been (re)established since
   * the last negotiation — either of its two pooled sockets being new counts (KIP-35 rule 7),
   * since version information is only valid for the connection it was learned on. The resulting
   * table is shared by both pooled sockets.
   *
   * @public
   * @returns {Promise}
   */
  async connect() {
    await this.lock.acquire()
    try {
      if (this.isConnected()) {
        return
      }

      const connection = await this.connectionPool.getConnection()

      // Read once: this is the generation the negotiated table below is learned at, not
      // whatever the pool happens to report after the round trip (a concurrent socket coming
      // up mid-negotiation must not be attributed to this table).
      const generation = this.connectionPool.generation
      if (this.versionsGeneration !== generation) {
        this.versions = await this.apiVersions()
        this.versionsGeneration = generation
      }

      this.connectionPool.setVersions(this.versions)
      this.lookupRequest = lookup(this.versions)
      this[PRIVATE.CAPPED_LOOKUPS].clear()

      await connection.authenticate()
    } finally {
      await this.lock.release()
    }
  }

  /**
   * @public
   * @returns {Promise}
   */
  async disconnect() {
    await this.connectionPool.destroy()
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
    const bestImplementedVersion = Math.max(...requests.ApiVersions.versions.map(Number))

    try {
      return toVersionMap(await this[PRIVATE.API_VERSIONS_AT](bestImplementedVersion))
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

      return toVersionMap(await this[PRIVATE.API_VERSIONS_AT](retryVersion))
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
   * @public
   * @param {number} apiKey
   * @param {object} definition the entry from `requests`
   * @param {{ [apiKey: number]: number }} [maxVersions] the same optional ceiling `lookup` takes
   * @returns {number|null}
   */
  negotiatedVersion(apiKey, definition, maxVersions) {
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
   * decision — a fetch downgraded to v12 while a topic id is unknown, a produce capped at v11
   * inside a transaction — and rebuilding the selector per request would allocate on it. In
   * practice this map holds at most two entries per `Broker` instance. It is cleared wherever
   * `lookupRequest` or `versionsGeneration` is assigned, so it can never outlive the version
   * table it was derived from.
   *
   * @public
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
   */
  async [PRIVATE.API_VERSIONS_AT](version) {
    const apiVersions = requests.ApiVersions.protocol({ version })
    return await this[PRIVATE.SEND_REQUEST]({
      ...apiVersions(),
      requestTimeout: this.connectionPool.connectionTimeout,
    })
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
   */
  async metadata(topics = []) {
    const metadata = this.lookupRequest(apiKeys.Metadata, requests.Metadata)
    const shuffledTopics = shuffle(topics)
    return await this[PRIVATE.SEND_REQUEST](
      metadata({ topics: shuffledTopics, allowAutoTopicCreation: this.allowAutoTopicCreation })
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
   * @returns {Promise}
   */
  async produce({
    topicData,
    transactionalId,
    producerId,
    producerEpoch,
    acks = -1,
    timeout = 30000,
    compression = Compression.None,
  }) {
    const produce = this.lookupRequest(apiKeys.Produce, requests.Produce)
    return await this[PRIVATE.SEND_REQUEST](
      produce({
        acks,
        timeout,
        compression,
        topicData,
        transactionalId,
        producerId,
        producerEpoch,
      })
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
   *                           (Java's `check.crcs`). A mismatch throws `KafkaJSPartialMessageError` with
   *                           `crcMismatch: true` instead of silently accepting the batch.
   * @param {Map<string, string>} [request.topicIds] topic name -> topic id, for every topic in
   *   this request. A request that supplies none is capped at the last version keyed by name, so a
   *   caller that knows nothing about topic ids keeps working unchanged.
   * @param {number} [request.maxVersion] an additional ceiling; the lower of the two wins.
   * @returns {Promise}
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

    return await this[PRIVATE.SEND_REQUEST](
      fetch({
        replicaId,
        isolationLevel,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics: consolidatedTopicPartitions,
        topicIds,
        rackId,
        checkCrcs,
      })
    )
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
   * @public
   * @param {object} request
   * @param {string} request.groupId The unique group id
   * @param {import("../protocol/coordinatorTypes").CoordinatorType} request.coordinatorType The type of coordinator to find
   * @returns {Promise}
   */
  async findGroupCoordinator({ groupId, coordinatorType }) {
    // TODO: validate groupId, mandatory
    const findCoordinator = this.lookupRequest(apiKeys.GroupCoordinator, requests.GroupCoordinator)
    return await this[PRIVATE.SEND_REQUEST](findCoordinator({ groupId, coordinatorType }))
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

    try {
      return await makeRequest()
    } catch (error) {
      if (error.name === 'KafkaJSMemberIdRequired') {
        return makeRequest(error.memberId)
      }

      throw error
    }
  }

  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId
   * @param {string} request.memberId
   * @param {string} [request.groupInstanceId=null] Static membership id (KIP-345). A static member
   *                                does not send LeaveGroup on a normal shutdown (see
   *                                `Runner#stop`); this is only reached for an explicit leave.
   * @returns {Promise}
   */
  async leaveGroup({ groupId, memberId, groupInstanceId }) {
    const leaveGroup = this.lookupRequest(apiKeys.LeaveGroup, requests.LeaveGroup)
    return await this[PRIVATE.SEND_REQUEST](leaveGroup({ groupId, memberId, groupInstanceId }))
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
   * @param {number} [request.sessionTimeout] Used to derive the SyncGroup request timeout
   * @param {number} [request.rebalanceTimeout] Used to derive the SyncGroup request timeout
   * @returns {Promise}
   */
  async syncGroup({
    groupId,
    generationId,
    memberId,
    groupInstanceId,
    groupAssignment,
    sessionTimeout,
    rebalanceTimeout,
  }) {
    const syncGroup = this.lookupRequest(apiKeys.SyncGroup, requests.SyncGroup)
    return await this[PRIVATE.SEND_REQUEST](
      syncGroup({
        groupId,
        generationId,
        memberId,
        groupInstanceId,
        groupAssignment,
        sessionTimeout,
        rebalanceTimeout,
      })
    )
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
   *                        { partition: 0, offset: '11' }
   *                      ]
   *                    }
   *                  ]
   * @returns {Promise}
   */
  async offsetCommit({
    groupId,
    groupGenerationId,
    memberId,
    groupInstanceId,
    retentionTime,
    topics,
  }) {
    const offsetCommit = this.lookupRequest(apiKeys.OffsetCommit, requests.OffsetCommit)
    return await this[PRIVATE.SEND_REQUEST](
      offsetCommit({
        groupId,
        groupGenerationId,
        memberId,
        groupInstanceId,
        retentionTime,
        topics,
      })
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
   * @returns {Promise}
   */
  async offsetFetch({ groupId, topics }) {
    const offsetFetch = this.lookupRequest(apiKeys.OffsetFetch, requests.OffsetFetch)
    return await this[PRIVATE.SEND_REQUEST](offsetFetch({ groupId, topics }))
  }

  /**
   * @public
   * @param {object} request
   * @param {Array} request.groupIds
   * @returns {Promise}
   */
  async describeGroups({ groupIds }) {
    const describeGroups = this.lookupRequest(apiKeys.DescribeGroups, requests.DescribeGroups)
    return await this[PRIVATE.SEND_REQUEST](describeGroups({ groupIds }))
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
   * @returns {Promise}
   */
  async describeConfigs({ resources, includeSynonyms = false }) {
    const describeConfigs = this.lookupRequest(apiKeys.DescribeConfigs, requests.DescribeConfigs)
    return await this[PRIVATE.SEND_REQUEST](describeConfigs({ resources, includeSynonyms }))
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
   * Send an `InitProducerId` request to fetch a PID and bump the producer epoch.
   *
   * Request should be made to the transaction coordinator.
   * @public
   * @param {object} request
   * @param {number} request.transactionTimeout The time in ms to wait for before aborting idle transactions
   * @param {number} [request.transactionalId] The transactional id or null if the producer is not transactional
   * @returns {Promise}
   */
  async initProducerId({ transactionalId, transactionTimeout }) {
    const initProducerId = this.lookupRequest(apiKeys.InitProducerId, requests.InitProducerId)
    return await this[PRIVATE.SEND_REQUEST](initProducerId({ transactionalId, transactionTimeout }))
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
   * @returns {Promise}
   */
  async addPartitionsToTxn({ transactionalId, producerId, producerEpoch, topics }) {
    const addPartitionsToTxn = this.lookupRequest(
      apiKeys.AddPartitionsToTxn,
      requests.AddPartitionsToTxn
    )
    return await this[PRIVATE.SEND_REQUEST](
      addPartitionsToTxn({ transactionalId, producerId, producerEpoch, topics })
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
   * @returns {Promise}
   */
  async addOffsetsToTxn({ transactionalId, producerId, producerEpoch, groupId }) {
    const addOffsetsToTxn = this.lookupRequest(apiKeys.AddOffsetsToTxn, requests.AddOffsetsToTxn)
    return await this[PRIVATE.SEND_REQUEST](
      addOffsetsToTxn({ transactionalId, producerId, producerEpoch, groupId })
    )
  }

  /**
   * Send a `TxnOffsetCommit` request to persist the offsets in the `__consumer_offsets` topics.
   *
   * Request should be made to the consumer coordinator.
   * @public
   * @param {object} request
   * @param {OffsetCommitTopic[]} request.topics
   * @param {string} request.transactionalId The transactional id corresponding to the transaction.
   * @param {string} request.groupId The unique group identifier (for the consumer group)
   * @param {number} request.producerId Current producer id in use by the transactional id.
   * @param {number} request.producerEpoch Current epoch associated with the producer id.
   * @param {OffsetCommitTopic[]} request.topics
   *
   * @typedef {Object} OffsetCommitTopic
   * @property {string} topic
   * @property {OffsetCommitTopicPartition[]} partitions
   *
   * @typedef {Object} OffsetCommitTopicPartition
   * @property {number} partition
   * @property {number} offset
   * @property {string} [metadata]
   *
   * @returns {Promise}
   */
  async txnOffsetCommit({ transactionalId, groupId, producerId, producerEpoch, topics }) {
    const txnOffsetCommit = this.lookupRequest(apiKeys.TxnOffsetCommit, requests.TxnOffsetCommit)
    return await this[PRIVATE.SEND_REQUEST](
      txnOffsetCommit({ transactionalId, groupId, producerId, producerEpoch, topics })
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
   * @returns {Promise}
   */
  async endTxn({ transactionalId, producerId, producerEpoch, transactionResult }) {
    const endTxn = this.lookupRequest(apiKeys.EndTxn, requests.EndTxn)
    return await this[PRIVATE.SEND_REQUEST](
      endTxn({ transactionalId, producerId, producerEpoch, transactionResult })
    )
  }

  /**
   * Send request for list of groups
   * @public
   * @returns {Promise}
   */
  async listGroups() {
    const listGroups = this.lookupRequest(apiKeys.ListGroups, requests.ListGroups)
    return await this[PRIVATE.SEND_REQUEST](listGroups())
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
   * @private
   */
  async [PRIVATE.SEND_REQUEST](protocolRequest) {
    try {
      return await this.connectionPool.send(protocolRequest)
    } catch (e) {
      if (e.name === 'KafkaJSConnectionClosedError') {
        await this.disconnect()
      }

      throw e
    }
  }
}
