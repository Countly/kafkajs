const {
  currentDeadline,
  checked,
  shareMetadata,
  sharedSetup,
  retrierOf,
  waitOf,
  withAdminGuard,
} = require('../admin/deadline')
const { isNativeError } = require('util').types
const describeThrown = require('../network/describeThrown')
const Broker = require('../broker')
const { NodeSetupStates } = require('../network/connectionSetup')
const MetadataCache = require('./metadataCache')
const createRetry = require('../retry')
const shuffle = require('../utils/shuffle')
const { CLUSTER_REBOOTSTRAP } = require('./instrumentationEvents')
const {
  KafkaJSBrokerNotFound,
  KafkaJSConnectionError,
  KafkaJSProtocolError,
  KafkaJSNonRetriableError,
} = require('../errors')
const { isConnectionFailure } = require('../network/connectionFailure')
const { failure, createErrorFromCode } = require('../protocol/error')
const { checkPartitions } = require('../protocol/requests/metadata')

/** The code a consumer's named refresh isolates for an established target. */
const UNKNOWN_TOPIC_OR_PARTITION = 3
/** How long an isolated target counts as known before it is asked for again, at most. */
const ISOLATED_RECHECK_MS = 1000
/** A metadata request whose cluster a rebootstrap replaced before its outcome took effect. */
const SUPERSEDED = Symbol('superseded')
/** A metadata answer that a newer accepted answer overtook while its pools were built. */
const OVERTAKEN = Symbol('overtaken')
/** The `code` of the retriable error a selection fails with when its broker was replaced. */
const SELECTION_SUPERSEDED = 'SELECTION_SUPERSEDED'

/**
 * Names one installed metadata answer: its place in acceptance order, the rebootstrap generation
 * and the disconnect count it was installed under.
 * @typedef {{publication: number, recovery: number, disconnects: number}} MetadataReceipt
 */

/** Topic errors that drop an earlier producer target the broker no longer serves. */
const DROPPED_TOPIC_ERRORS = new Set([
  'UNKNOWN_TOPIC_OR_PARTITION',
  'INVALID_TOPIC_EXCEPTION',
  'TOPIC_AUTHORIZATION_FAILED',
])

const { keys, assign, values } = Object

/**
 * @typedef {object} TopicMetadataStatus
 * @property {string} topic a consumer target
 * @property {'unavailable'|'available'} status
 * @property {string|null} type `UNKNOWN_TOPIC_OR_PARTITION` when unavailable, null when available
 * @property {number} code 3 when unavailable, 0 when available
 */

/**
 * @typedef {object} TargetEstablishment
 * @property {(topic: string) => boolean} isEstablished whether a metadata answer accepted the
 *   topic for its current declaration, or it is a first-add row
 * @property {(topic: string) => boolean} isTarget whether the topic is still a target
 * @property {(topics: string[]) => void} establish records the topics an answer accepted, unless a
 *   target was removed since the capture
 */

/**
 * Whether a cached broker now has another address. A rack is not part of a broker's identity: a
 * new rack is written onto the cached connection pool in place, with no new connection.
 * @param {Broker} broker
 * @param {{ host: string, port: number }} address
 * @returns {boolean}
 */
const hasBrokerMoved = (broker, { host, port }) =>
  broker.connectionPool.host !== host || broker.connectionPool.port !== port

/**
 * @param {Broker|undefined} broker
 * @param {{ host: string, port: number }} address
 * @returns {boolean} whether the broker exists and is at the address
 */
const atAddress = (broker, address) => !!broker && !hasBrokerMoved(broker, address)

/**
 * @param {Error} [cause] the failure the selection met, if any
 * @returns {KafkaJSConnectionError} retriable: the selected broker was replaced or disconnected
 */
const selectionSuperseded = (cause) =>
  new KafkaJSConnectionError('The broker was replaced while it was selected', {
    code: SELECTION_SUPERSEDED,
    cause,
  })

/**
 * The broker option of a producer lifecycle's private pre-write check, as an argument list: none
 * without a lifecycle, so an unfenced call keeps the shape it always had.
 * @param {import('../producer/generation')} [fence]
 * @returns {[{ guard: () => Error|null }]|[]}
 */
const guardArgs = (fence) => (fence ? [{ guard: fence.guard }] : [])

/**
 * The lifecycle a recovery checks, rebuilt from the private pre-write check a broker was called
 * with: a broker is handed `fence.guard` and nothing else. It answers only `closed` and `check()`,
 * all a rebootstrap asks of its fence, and keeps `guard` so recoveries of one lifecycle share.
 * @param {() => Error|null} guard the check the failed `connect()` was called with
 * @returns {{ guard: () => Error|null, closed: boolean, check: () => void }}
 */
const fenceOf = (guard) => ({
  guard,
  get closed() {
    return Boolean(guard())
  },
  check() {
    const refusal = guard()
    if (refusal) throw refusal
  },
})

// Bounds the `.cause` walk below: no currently-reachable chain is anywhere near this deep, so it
// only guards against a future, accidentally self-referential `cause` looping forever.
const MAX_CAUSE_CHAIN_DEPTH = 10

/**
 * True when `error`, or anything in its `cause` chain, is a rejected SASL credential
 * (`KafkaJSSASLAuthenticationError`) - matches the `getOriginalCause` idiom `src/consumer/index.js`
 * uses for the same purpose. A bad credential means the broker is fine and reachable; only this
 * client is at fault, which no rebootstrap can fix.
 * @param {Error|null|undefined} error
 * @returns {boolean}
 */
const isAuthenticationFailure = (error) => {
  let current = error
  for (let depth = 0; current != null && depth < MAX_CAUSE_CHAIN_DEPTH; depth += 1) {
    if (current.name === 'KafkaJSSASLAuthenticationError') {
      return true
    }
    current = current.cause
  }
  return false
}

/** @type {WeakMap<object[], Map<string, object>>} see `BrokerPool.topicRowsByName` */
const topicRowIndexes = new WeakMap()

const PRIVATE = {
  REBOOTSTRAP: Symbol('private:BrokerPool:rebootstrap'),
}

module.exports = class BrokerPool {
  /**
   * Owns broker readiness and retained routing responses; optional producer captures fence topic merges.
   * @param {object} options
   * @param {object|null} [options.telemetryMetrics=null] private numeric observer of this client,
   *   never shared with the dedicated telemetry transport
   * @param {import("./connectionPoolBuilder").ConnectionPoolBuilder} options.connectionPoolBuilder
   * @param {import("./bootstrapResolution")|null} [options.bootstrapResolution] Cluster-owned
   *   readiness gate; omitted when disabled to preserve the synchronous connection path
   * @param {import("../../types").Logger} options.logger
   * @param {import("./metadataCache")} [options.metadataCache] optional: a `BrokerPool` owns its
   *   metadata and is usable standalone, so it defaults to a private cache rather than requiring
   *   a collaborator it can build itself. `Cluster` always injects its own, which is then the
   *   single instance every route reads.
   * @param {(topics: string[]) => ((topic: string) => boolean)} [options.captureTopics] scoped producer publication predicate
   * @param {(topic: string, options?: {publish?: boolean}) => boolean} [options.dropTopic] required when `captureTopics` is
   *   given: forgets a producer target the broker no longer serves, unless producer work still
   *   owns it; returns whether it did
   * @param {(topic: string) => boolean} [options.isNewTarget] required when `captureTopics` is
   *   given: whether a producer target is being added by the call in progress, so its refusal
   *   fails that call instead of being dropped
   * @param {(status: TopicMetadataStatus) => void} [options.onTopicMetadataStatus] given only
   *   for a consumer's cluster: its named refreshes isolate an established target answered
   *   `UNKNOWN_TOPIC_OR_PARTITION` (see `isolateUnknownTopics`) and report each change of that
   *   topic's status here
   * @param {() => TargetEstablishment} [options.captureEstablishment] required with
   *   `onTopicMetadataStatus`: captures, when a named refresh begins, which targets a metadata
   *   answer has accepted (see `Cluster#captureEstablishment`)
   * @param {import("../instrumentation/emitter")|null} [options.instrumentationEmitter]
   * @param {import("../../types").RetryOptions} [options.retry]
   * @param {number} [options.drainTimeout] obsolete-resource drain bound in milliseconds
   * @param {boolean} [options.allowAutoTopicCreation]
   * @param {number} [options.authenticationTimeout]
   * @param {number} [options.metadataMaxAge]
   * @param {'rebootstrap'|'none'} [options.metadataRecoveryStrategy='rebootstrap'] KIP-899/1102:
   *   `'rebootstrap'` enables all three triggers in `refreshMetadata`/`findConnectedBroker`, and
   *   the same recovery on a `REBOOTSTRAP_REQUIRED` answer to ApiVersions (KIP-1242);
   *   `'none'` disables them, sends no cluster or node id in ApiVersions and leaves
   *   `REBOOTSTRAP_REQUIRED` (129) as a plain protocol error.
   * @param {boolean} [options.metadataClusterCheck=true] `false` makes every broker of this pool
   *   send no cluster or node id in ApiVersions v5; recovery on `REBOOTSTRAP_REQUIRED` is unchanged.
   * @param {number} [options.metadataRecoveryRebootstrapTriggerMs=300000] how long metadata must
   *   fail *continuously* (trigger (a)) before a rebootstrap fires.
   */
  constructor({
    telemetryMetrics = null,
    connectionPoolBuilder,
    bootstrapResolution = null,
    logger,
    metadataCache,
    captureTopics,
    dropTopic,
    isNewTarget,
    onTopicMetadataStatus,
    captureEstablishment,
    instrumentationEmitter,
    retry,
    allowAutoTopicCreation,
    authenticationTimeout,
    metadataMaxAge,
    metadataRecoveryStrategy,
    metadataClusterCheck,
    metadataRecoveryRebootstrapTriggerMs,
    drainTimeout,
  }) {
    this.captureTopics = captureTopics
    this.dropTopic = dropTopic
    this.isNewTarget = isNewTarget
    this.onTopicMetadataStatus = onTopicMetadataStatus
    this.captureEstablishment = captureEstablishment
    /** @type {Set<string>|null} the isolated targets: never more than the current targets */
    this.unavailableTopics = onTopicMetadataStatus ? new Set() : null
    /** When an isolated target stops counting as known to `refreshMetadataIfNecessary`. */
    this.isolatedRecheckAt = 0
    this.telemetryMetrics = telemetryMetrics
    this.telemetryNodeRevision = 0n
    this.drainTimeout = drainTimeout
    this.retiredBrokers = new WeakSet()
    /**
     * Brokers a disconnect or a failure removal detached, each with the settlement of its drains,
     * until those settle: a later disconnect reaches their sockets again and awaits them.
     * @type {Map<import('../broker'), Promise<unknown>>}
     */
    this.draining = new Map()
    this.rootLogger = logger
    this.connectionPoolBuilder = connectionPoolBuilder
    this.bootstrapResolution = bootstrapResolution
    this.metadataMaxAge = metadataMaxAge || 0
    this.logger = logger.namespace('BrokerPool')
    this.retryOptions = assign({}, retry)
    this.retrier = (action) => retrierOf(this.retryOptions)(action)
    this.authenticationTimeout = authenticationTimeout ?? 10000
    this.metadataCache = metadataCache ?? new MetadataCache({ logger })
    this.instrumentationEmitter = instrumentationEmitter ?? null

    // Broker address -> blackout entry, shared by every ConnectionPool this BrokerPool builds
    // (see buildConnectionPool), so a pool rebuilt after a connection error (seed rotation,
    // illegal SASL state) keeps the blackout instead of starting over with an empty one.
    this.authenticationBlackout = new Map()
    this.setupStates = null
    this.setupMetadataEndpoints = new Map()

    this.telemetryAdvertisementListener = null
    /**
     * Every broker of this pool is built here, so each one names the identity the pool currently
     * holds in its ApiVersions requests, follows the pool's recovery strategy and reports a
     * REBOOTSTRAP_REQUIRED answer to `onApiVersionsFailure`.
     * @param {object} options the `Broker` options the call site chooses
     * @returns {Broker}
     */
    this.createBroker = (options) => {
      const broker = new Broker({
        onReady: () => {
          this.bindTelemetryNode(broker)
          this.telemetryAdvertisementListener?.()
        },
        allowAutoTopicCreation,
        authenticationTimeout,
        metadataRecoveryStrategy: this.metadataRecoveryStrategy,
        metadataClusterCheck: this.metadataClusterCheck,
        metadataIdentity: () => ({
          clusterId: this.metadata?.clusterId ?? null,
          nodeId: broker.nodeId ?? -1,
        }),
        onApiVersionsFailure: (error, source) => this.onApiVersionsFailure(error, source),
        ...options,
      })
      this.bindTelemetryNode(broker)
      return broker
    }

    this.brokers = {}
    this.telemetryEndpointRevision = 0n
    this.telemetryAdvertisements = new WeakMap()
    /** @type {Broker | undefined} */
    this.seedBroker = undefined
    /** @type {import("../../types").BrokerMetadata | null} */
    this.metadata = null
    this.metadataExpireAt = null
    /** Counts `disconnect()` calls: a refresh started before one installs nothing after it. */
    this.disconnects = 0
    /** Counts completed rebootstraps: a metadata request sent before one installs nothing after it. */
    this.recoveries = 0
    /** Counts the accepted metadata answers, so an answer can tell a newer one overtook it. */
    this.metadataPublications = 0
    /** @type {{publication: number, settled: boolean, done: Promise<void>}|null} the newest accepted answer */
    this.acceptedAnswer = null
    /** @type {MetadataReceipt|null} the installed answer; null once a rebootstrap or disconnect cleared it */
    this.installedMetadata = null
    /**
     * The newest refresh of each subscription scope, kept while any refresh of that scope runs, so
     * an older one can still read its outcome.
     * @type {WeakMap<object, {newest: object, active: number}>}
     */
    this.scopeRefreshes = new WeakMap()
    /** @type {import('../consumer/subscription')|null} default scope of a client-pattern consumer */
    this.consumerMetadataScope = null
    /** Advances when the default scope is registered or released, fencing earlier unscoped work. */
    this.consumerMetadataGeneration = 0
    /** Whether the consumer subscribes by a broker-side regex; no lookup by name may create then. */
    this.consumerBrokerRegex = false
    this.versions = null

    this.metadataRecoveryStrategy = metadataRecoveryStrategy ?? 'rebootstrap'
    this.metadataClusterCheck = metadataClusterCheck ?? true
    this.metadataRecoveryRebootstrapTriggerMs = metadataRecoveryRebootstrapTriggerMs ?? 300000

    /** Failures `onApiVersionsFailure` already recovered from, so a surrounding catch skips them. */
    this.recoveredFailures = new WeakSet()

    /** Set at the first failed metadata attempt following a success; cleared on any success. */
    this.metadataUnavailableSince = null
    /**
     * Kept as internal state, not read by either trigger: an earlier design additionally bounded
     * trigger (a)'s restart and trigger (c) against this timestamp, but `metadataUnavailableSince`
     * (cleared by every `rebootstrap()`) and `rebootstrapArmed` (armed only by a post-rebootstrap
     * success) already cap each trigger's rate on their own, which made the extra bound
     * redundant rather than stricter. Available for a future bound or for surfacing on the
     * `REBOOTSTRAP` event payload; neither is done yet.
     */
    this.lastRebootstrapAt = null
    /** A successful metadata response since the last rebootstrap arms triggers (a) and (c). */
    this.rebootstrapArmed = false

    this[PRIVATE.REBOOTSTRAP] = shareMetadata(
      async (reason, { fence } = {}) => {
        fence?.check()
        checked(null)
        const seed = this.seedBroker
        const table = this.brokers
        const installedMetadata = this.installedMetadata
        const brokers = new Set(values(table))
        // Only an answer installed meanwhile shows the cluster answers; one still preparing does not.
        const ownsRouting = () =>
          this.brokers === table && this.installedMetadata === installedMetadata
        await this.createSeedBroker({
          fence,
          isCurrent: () => ownsRouting() && this.seedBroker === seed,
          // Committed in the tick the new seed is installed, so no answer sees one without the other.
          install: (installed) => {
            this.recoveries += 1
            this.metadata = null
            this.metadataExpireAt = null
            this.installedMetadata = null
            this.setupMetadataEndpoints.clear()
            // `createSeedBroker` retired the replaced seed just before this step.
            for (const broker of new Set([...brokers, ...values(this.brokers)])) {
              if (broker !== installed && broker !== seed) this.closeObsolete(broker)
            }
            this.retainSetupEndpoints()
            this.lastRebootstrapAt = Date.now()
            this.metadataUnavailableSince = null
            this.rebootstrapArmed = false
            // Routing listeners hear of the clear once the old brokers and endpoints are gone.
            this.metadataCache.clear()
            this.logger.warn('Rebootstrapping the client from the configured broker list', {
              reason,
            })
            this.emit(CLUSTER_REBOOTSTRAP, { reason })
          },
        })
        // The recovery is complete; a lifecycle that closed meanwhile still gets its close error.
        fence?.check()
        checked(null)
      },
      (reason, { fence } = {}) => fence?.guard ?? fence
    )
  }

  /**
   * Drops every connection and every piece of cluster state derived from metadata, then rebuilds
   * the seed broker from the configured broker list.
   *
   * Deliberately does **not** call `disconnect()`: that clears `authenticationBlackout`, and a
   * rebootstrap must never hand a broker that just rejected our credentials a fresh, immediate
   * authentication attempt. The negotiated `versions` table is kept for the same reason it is
   * kept across a reconnect - it belongs to the broker software, not to the metadata.
   *
   * In the same synchronous step that installs the replacement seed, `recoveries` advances, the
   * metadata is cleared, the old brokers are detached and retired (those held when it began and any
   * added while the seed was built, since they came from the old cluster; aliases deduplicated)
   * and the old cluster's setup endpoints are forgotten; routing listeners are told last. So no
   * metadata answer sees the new seed with the old count, and an answer to a request sent before
   * is never installed (see `refreshMetadata`). Draining proceeds in the background under the
   * obsolete-resource bound.
   *
   * Concurrent callers join one in-flight rebootstrap, except that a producer lifecycle's call
   * joins only calls of the same lifecycle. Once that lifecycle closed, its rebootstrap tears
   * nothing down, retires a pool built meanwhile unused, and publishes no clock reset or event.
   *
   * **Re-entrancy invariant**: this is awaited from two places - the whole-body catch in
   * `refreshMetadata`, and `findConnectedBroker()` inside that method's retry callback - which
   * are different promises, so there is no deadlock. It must stay that way: nothing this method
   * calls (`createSeedBroker()` included) may call back into `findConnectedBroker` or
   * `refreshMetadata`, because a caller re-entering its own shared promise would await itself.
   *
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-899%3A+Allow+producer+and+consumer+clients+to+rebootstrap
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1102%3A+Enable+clients+to+rebootstrap+based+on+timeout+or+error+code
   * @public
   * @param {'timeout'|'required'|'all-brokers-unavailable'} reason
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking
   * @returns {Promise<void>}
   * @throws {Error} the lifecycle's close error
   */
  async rebootstrap(reason, { fence } = {}) {
    return this[PRIVATE.REBOOTSTRAP](reason, { fence })
  }

  /**
   * The retrier of work that belongs to one producer lifecycle: it stops at once, with the
   * lifecycle's close error, when the lifecycle closes. Counts and delays are the pool's own.
   * @public
   * @param {import('../producer/generation')} [fence] the captured producer lifecycle
   * @returns {import('../retry').Retrier} the pool's own retrier when there is no lifecycle
   */
  retryFor(fence) {
    return fence ? createRetry({ ...this.retryOptions, fence }) : this.retrier
  }

  /** Reconciles eligible endpoints while retaining active attempts. @returns {void} */
  retainSetupEndpoints() {
    this.setupStates?.retain([
      ...(this.connectionPoolBuilder.bootstrapEndpoints || []),
      ...this.setupMetadataEndpoints.values(),
    ])
  }

  /**
   * Binds a current known node outside response completion; a bootstrap endpoint without a node id
   * gets a null slot, so successful connects count but no node gauge appears. Each concrete pool
   * receives one non-reused local endpoint revision.
   * @param {import('../broker')} broker current broker and its concrete pool
   * @returns {void} observer failures cannot change metadata publication or core outcomes
   */
  bindTelemetryNode(broker) {
    if (!this.telemetryMetrics) return
    try {
      const pool = broker.connectionPool
      if (pool.telemetryNodeRevision === undefined) {
        pool.telemetryNodeRevision = ++this.telemetryNodeRevision
      }
      const slot = this.telemetryMetrics.bindNode({
        nodeId: broker.nodeId,
        endpoint: `${pool.host}:${pool.port}`,
        endpointRevision: pool.telemetryNodeRevision,
      })
      pool.telemetrySlot = slot
      for (const connection of pool.pool) {
        connection.setTelemetryObserver(this.telemetryMetrics, slot, pool)
      }
    } catch {}
  }

  /**
   * Reissues slots after transient collector reset for current normal pools, including ready
   * sockets that need no new setup. Retired pools and the dedicated telemetry pool stay fenced.
   * @returns {void} bounded to the current broker inventory; no network or request-path work
   */
  rebindTelemetryNodes() {
    if (!this.telemetryMetrics) return
    const brokers = new Set([this.seedBroker, ...Object.values(this.brokers)])
    for (const broker of brokers) {
      if (
        broker &&
        !this.retiredBrokers.has(broker) &&
        !broker.retired &&
        !broker.connectionPool.retired
      )
        this.bindTelemetryNode(broker)
    }
  }

  /**
   * Retires a pool's node identity so late responses of its sockets cannot relabel a replacement.
   * @private
   * @param {import('../network/connectionPool')} pool pool leaving service
   * @returns {void} contained; never changes the caller's outcome
   */
  retireTelemetryNode(pool) {
    if (!this.telemetryMetrics || !pool.telemetrySlot) return
    try {
      this.telemetryMetrics.retireNode(pool.telemetrySlot)
    } catch {}
  }

  /**
   * Builds a connection pool via the injected builder and stamps it with this client's
   * authenticationTimeout and this BrokerPool's shared authenticationBlackout map. Every pool
   * this BrokerPool creates goes through here so a pool rebuilt after a connection error keeps
   * both, instead of a fresh pool silently losing the blackout (and falling back to the 1s
   * floor for its window) the way a bare `connectionPoolBuilder.build()` call would.
   *
   * @private
   * @param {{ host?: string, port?: number, rack?: string }} [destination]
   * @param {object} [options] captured installation ownership
   * @param {import('../producer/generation')} [options.fence] original producer lifecycle
   * @param {() => boolean} [options.isCurrent] routing identity checked before setup publication
   * @returns {Promise<import("../network/connectionPool")|null>} a current pool with shared setup
   *   state, or null after closing a stale routing build without publishing its setup inventory
   * @throws {Error} original lifecycle or discovery refusal; closes only the unused pool
   */
  async buildConnectionPool(destination, { fence, isCurrent = () => true } = {}) {
    const owner = currentDeadline()
    fence?.check()
    owner?.check()
    const connectionPool = await this.connectionPoolBuilder.build(destination)
    try {
      fence?.check()
      owner?.check()
    } catch (error) {
      this.closePool(connectionPool)
      throw error
    }
    if (!isCurrent()) {
      this.closePool(connectionPool)
      return null
    }
    connectionPool.authenticationTimeout = this.authenticationTimeout
    connectionPool.authenticationBlackout = this.authenticationBlackout
    if (!this.setupStates) this.setupStates = new NodeSetupStates(connectionPool.setupPolicy)
    connectionPool.setupStates = this.setupStates
    this.retainSetupEndpoints()
    return connectionPool
  }

  /**
   * Applies the broker endpoints a leadership error carried (KIP-951), so the client can talk to
   * a new leader without waiting for a metadata refresh.
   *
   * A `Broker` wraps a connection pool whose address is fixed at construction, so an endpoint
   * whose host or port differs from the cached one is a *replacement*, not an update: a new pool
   * is built (through `buildConnectionPool`, which carries the shared authentication blackout)
   * and the old broker is retired. The replacement is built with the same fields the
   * metadata refresh path uses, `versions` included, so the request the hint is trying to make
   * fast does not pay a fresh ApiVersions round trip. A new rack alone is written in place.
   *
   * The old broker is retired in the background. It refuses new work while requests already
   * in flight can finish up to drainTimeout, then its sockets close and queued work rejects.
   * Recovery never waits for that drain.
   *
   * An unknown node id at the seed broker's address adopts the seed, as `refreshMetadata`'s reduce
   * does, so the seed's address is not connected twice; a seed the table already holds under
   * another id is never adopted again. `Cluster#findGroupCoordinator` adds a coordinator it
   * reached through FindCoordinator this way too.
   *
   * Three cases never build a bare replacement:
   * - **The seed broker.** `refreshMetadata` can adopt `this.seedBroker` into `this.brokers` under
   *   a real node id; every other broker-pool method (`hasConnectedBrokers`, `connect`,
   *   `findConnectedBroker`'s fallback) reads `this.seedBroker` directly, so swapping that object
   *   out from under it would leave them holding a stale, disconnected reference. When the entry
   *   at `nodeId` *is* the seed, its connection pool is swapped in place instead - the seed's
   *   identity and `this.seedBroker` never change - and the pool it was using is drained
   *   separately, mirroring how `refreshMetadata`'s reduce keeps the seed object rather than
   *   rebuilding it when it recognises the address (see `refreshMetadata` above).
   * - **An endpoint with no usable address.** A blank host or a non-positive port is exactly the
   *   input `connectionPoolBuilder` treats as "no destination given" and answers with the next
   *   configured bootstrap broker - silently pointing this node id at an unrelated broker. Such an
   *   endpoint is skipped.
   * - **A world that moved on underneath one `await`.** Between reading the current broker and the
   *   built replacement pool becoming ready, a concurrent rebootstrap/disconnect can empty
   *   `this.brokers` or install another seed, or a second hint for the same node id can already
   *   have written its own replacement. The table (and, for the seed, `this.seedBroker`) is
   *   re-checked before writing; on a mismatch the freshly built pool - never handed to a
   *   `Broker` - is retired instead of overwriting whatever is there now.
   *
   * Adding an unknown node id is allowed; removing one is not - only a metadata response may
   * shrink the broker table.
   *
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimisations+for+the+client
   * @public
   * @param {{ nodeId: number, host: string, port: number, rack: string|null }[]} [nodeEndpoints]
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] captured producer lifecycle; a pool
   *   completed after its close is retired unused and changes no routing state
   * @returns {Promise<void>}
   * @throws {Error} the lifecycle's close error
   */
  async applyNodeEndpoints(nodeEndpoints, { fence } = {}) {
    fence?.check()
    checked(null)
    if (!nodeEndpoints || nodeEndpoints.length === 0) {
      return
    }

    for (const { nodeId, host, port, rack } of nodeEndpoints) {
      if (!host || !Number.isInteger(port) || port <= 0) {
        continue
      }

      const broker = this.brokers[nodeId]

      if (broker && !hasBrokerMoved(broker, { host, port })) {
        broker.connectionPool.rack = rack
        continue
      }

      if (
        !broker &&
        this.seedBroker &&
        !values(this.brokers).includes(this.seedBroker) &&
        !hasBrokerMoved(this.seedBroker, { host, port })
      ) {
        this.seedBroker.nodeId = nodeId
        this.bindTelemetryNode(this.seedBroker)
        this.seedBroker.connectionPool.rack = rack
        this.brokers[nodeId] = this.seedBroker
        continue
      }

      const isSeed = broker !== undefined && broker === this.seedBroker
      const isCurrent = () =>
        this.brokers[nodeId] === broker && (!isSeed || this.seedBroker === broker)
      const connectionPool = await this.buildConnectionPool(
        { host, port, rack },
        { fence, isCurrent }
      )
      if (!connectionPool) continue
      if (fence?.closed || currentDeadline()?.guard()) {
        this.closePool(connectionPool)
        fence?.check()
        checked(null)
      }

      if (this.brokers[nodeId] !== broker || (isSeed && this.seedBroker !== broker)) {
        this.closePool(connectionPool)
        continue
      }

      if (isSeed) {
        this.replacePool(broker, connectionPool)
        this.setupMetadataEndpoints.set(String(nodeId), `${host}:${port}`)
        this.retainSetupEndpoints()
        continue
      }

      this.brokers[nodeId] = this.createBroker({
        logger: this.rootLogger,
        versions: this.versions,
        connectionPool,
        nodeId,
      })
      this.closeObsolete(broker)
      this.setupMetadataEndpoints.set(String(nodeId), `${host}:${port}`)
      this.retainSetupEndpoints()
    }
  }

  /**
   * Detaches only aliases of this obsolete identity and retires it once. The active seed is
   * shared with direct seed users and must be replaced before it can be retired.
   * @private
   * @param {import('../broker')|undefined} broker replaced broker
   * @returns {Promise<void>|undefined} the drain this call started, which it never waits for
   */
  closeObsolete(broker) {
    if (!broker || broker === this.seedBroker) return
    for (const nodeId of keys(this.brokers)) {
      if (this.brokers[nodeId] === broker) delete this.brokers[nodeId]
    }
    if (this.retiredBrokers.has(broker)) return
    this.retiredBrokers.add(broker)
    this.retireTelemetryNode(broker.connectionPool)
    const drain = broker.retire({ drainTimeout: this.drainTimeout })
    this.closeInBackground(drain)
    return drain
  }

  /**
   * Records one drain of a detached broker, joined with the earlier ones of that broker; the
   * broker leaves the record once all of them have settled.
   * @private
   * @param {import('../broker')} broker
   * @param {Promise<void>} drain
   * @returns {void}
   */
  trackDrain(broker, drain) {
    const all = Promise.allSettled([this.draining.get(broker), drain]).then(() => {
      if (this.draining.get(broker) === all) this.draining.delete(broker)
    })
    this.draining.set(broker, all)
  }

  /**
   * Installs an unused pool through the Broker's generation and retirement guards, then
   * retires the replaced pool without awaiting its drain. A retired Broker rejects the
   * offered pool itself and keeps its current pool attached.
   * @private
   * @param {import('../broker')} broker owner whose identity stays current
   * @param {import('../network/connectionPool')} pool unused replacement pool
   * @returns {void}
   */
  replacePool(broker, pool) {
    const previous = broker.connectionPool
    if (broker.replaceConnectionPool(pool)) {
      this.closePool(previous)
      this.bindTelemetryNode(broker)
    }
  }

  /**
   * Fences a replaced or never-installed pool, then drains it in the background.
   * @private
   * @param {import('../network/connectionPool')} pool obsolete pool
   * @returns {void}
   */
  closePool(pool) {
    this.retireTelemetryNode(pool)
    pool.retire()
    this.closeInBackground(pool.destroy({ drainTimeout: this.drainTimeout }))
  }

  /**
   * Lets a replaced broker, or the pool it replaced, finish closing without holding the caller
   * back. A failed close is logged when the logger can log it, and never surfaces: not as an
   * unhandled rejection, whatever the rejection reason is and even if the logger throws. The
   * closed object is no longer in the table, so there is nothing to retry.
   *
   * @private
   * @param {Promise<void>} closing
   * @returns {void}
   */
  closeInBackground(closing) {
    closing
      .catch((error) =>
        this.logger.warn('Closing a replaced broker connection failed', {
          error: error?.message ?? String(error),
        })
      )
      .catch(() => {})
  }

  /**
   * Copies only current ready foreground advertisements without discovery or connection work.
   * Revisions are issued on pool/generation/node changes and never reused on this owner.
   * @returns {ReadonlyArray<object>} immutable endpoint hints; no application-owned objects
   * @throws {Error} internal invalid advertisement; contained by the telemetry owner
   */
  telemetryCandidates() {
    const result = []
    const known = new Set([this.seedBroker, ...Object.values(this.brokers)])
    for (const broker of known) {
      if (!broker || broker.retired || !broker.isConnected()) continue
      const pool = broker.connectionPool
      if (pool.retired || broker.versionsGeneration !== pool.generation) continue
      let entry = this.telemetryAdvertisements.get(broker)
      if (
        !entry ||
        entry.pool !== pool ||
        entry.generation !== pool.generation ||
        entry.nodeId !== broker.nodeId ||
        entry.versions !== broker.versions
      ) {
        entry = {
          pool,
          generation: pool.generation,
          nodeId: broker.nodeId,
          versions: broker.versions,
          revision: ++this.telemetryEndpointRevision,
        }
        this.telemetryAdvertisements.set(broker, entry)
      }
      const versions = Object.freeze(
        Object.fromEntries(
          Object.entries(broker.versions).map(([key, range]) => [key, Object.freeze({ ...range })])
        )
      )
      result.push(
        Object.freeze({
          nodeId: broker.nodeId,
          host: pool.host,
          port: pool.port,
          versions,
          endpointRevision: entry.revision,
        })
      )
    }
    return Object.freeze(result)
  }

  /**
   * @public
   * @returns {Boolean}
   */
  hasConnectedBrokers() {
    const brokers = values(this.brokers)
    return (
      !!brokers.find((broker) => broker.isConnected()) ||
      (this.seedBroker ? this.seedBroker.isConnected() : false)
    )
  }

  /**
   * Builds a replacement seed broker from the next address the connection-pool builder offers,
   * installs it, and only then retires the one it replaced.
   *
   * The order matters: `this.seedBroker` is read without a lock by `connect()`'s retry callback,
   * `hasConnectedBrokers()` and `findConnectedBroker()`'s fallback, so it must never be unset -
   * nor point at a broker that is already tearing down - across the awaits in here.
   *
   * Concurrent initial builds adopt the first installed seed; replacement builds retain their
   * captured identity checks. A refused caller never adopts another caller's seed.
   *
   * @public
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: once
   *   it closed, a pool built meanwhile is retired unused instead of installed
   * @param {() => boolean} [options.isCurrent] captured recovery routing identity
   * @param {(seed: Broker) => void} [options.install] runs synchronously right after the new seed
   *   is installed and the replaced one retired; not run for an adopted seed
   * @returns {Promise<Broker>} installed seed
   * @throws {Error} original caller refusal or superseded recovery identity
   */
  async createSeedBroker({ fence, isCurrent = () => true, install } = {}) {
    fence?.check()
    checked(null)
    const previousSeedBroker = this.seedBroker
    const ownsSeed = () => isCurrent() && this.seedBroker === previousSeedBroker
    const connectionPool = await this.buildConnectionPool(undefined, { fence, isCurrent: ownsSeed })
    if (
      !connectionPool ||
      fence?.closed ||
      currentDeadline()?.guard() ||
      !isCurrent() ||
      this.seedBroker !== previousSeedBroker
    ) {
      if (connectionPool) this.closePool(connectionPool)
      fence?.check()
      checked(null)
      if (!previousSeedBroker && this.seedBroker && isCurrent()) return this.seedBroker
      throw new KafkaJSConnectionError('Seed broker build was superseded')
    }
    this.seedBroker = this.createBroker({
      connectionPool,
      logger: this.rootLogger,
    })
    this.closeObsolete(previousSeedBroker)
    install?.(this.seedBroker)
    return this.seedBroker
  }

  /**
   * Connects the seed broker, retrying and rotating through the configured broker list.
   *
   * The seed is re-read *inside* the retry callback: a rebootstrap or a `removeBroker()` can
   * replace (or, for `removeBroker`, unset) `this.seedBroker` between two scheduled attempts, so
   * a reference captured before the retrier would connect an object nothing else uses. A missing
   * seed is reported as a retriable connection error rather than dereferenced - the retrier
   * treats a `TypeError` as unrecoverable and would turn a transient window into a crash. An
   * attempt fulfils only when the seed it connected is still the seed; one replaced meanwhile (by
   * a rebootstrap, say) is neither rotated nor has its versions published, and the attempt fails
   * retriably.
   *
   * @public
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: its
   *   close ends the retries and opens no socket afterwards
   * @returns {Promise<void>}
   * @throws {Error} the lifecycle's close error
   */
  async connect({ fence } = {}) {
    fence?.check()
    checked(null)
    if (this.bootstrapResolution) {
      try {
        await this.bootstrapResolution.wait()
      } catch (error) {
        fence?.check()
        checked(null)
        throw error
      }
      fence?.check()
      checked(null)
    }
    if (this.hasConnectedBrokers()) {
      return
    }

    fence?.check()
    checked(null)
    if (!this.seedBroker) {
      await this.createSeedBroker({ fence })
    }

    return this.retryFor(fence)(async (bail, retryCount, retryTime) => {
      fence?.check()
      checked(null)
      const seedBroker = this.seedBroker

      if (!seedBroker) {
        throw new KafkaJSConnectionError('No seed broker available, the client is rebootstrapping')
      }

      try {
        await seedBroker.connect(...guardArgs(fence))
        fence?.check()
        checked(null)
        // A seed replaced meanwhile connected for nothing: the attempt is retried on its successor.
        if (this.seedBroker !== seedBroker) {
          throw new KafkaJSConnectionError('The seed broker was replaced while it connected')
        }
        this.versions = seedBroker.versions
      } catch (caught) {
        fence?.check()
        checked(null)
        const e = isNativeError(caught)
          ? caught
          : new KafkaJSNonRetriableError(describeThrown(caught), { cause: caught })
        // A seed replaced meanwhile (a rebootstrap retired it) is not rotated: its successor is current.
        if (
          this.seedBroker === seedBroker &&
          (isConnectionFailure(e) || describeThrown(e, 'type') === 'ILLEGAL_SASL_STATE')
        ) {
          // Connection builder will always rotate the seed broker
          await this.createSeedBroker({ fence })
          try {
            this.logger.error(
              `Failed to connect to seed broker, trying another broker from the list: ${describeThrown(e)}`,
              { retryCount, retryTime }
            )
          } catch {}
        } else {
          try {
            this.logger.error(describeThrown(e), { retryCount, retryTime })
          } catch {}
        }

        if (describeThrown(e, 'retriable') === 'true') throw e
        bail(e)
      }
    })
  }

  /**
   * Disconnects the seed, every broker in the table and every broker an earlier disconnect or a
   * failure removal detached that is still draining, starting all drains concurrently, so a
   * shorter `drainTimeout` reaches a drain already in progress. Detaches the captured table before
   * draining, and retires its non-seed brokers so a stale lookup cannot reconnect an orphan. The
   * seed stays reusable and owned. Work resumed during the drain builds a new table, which this
   * call leaves intact. Resolves once every drain of those brokers, earlier ones included, has
   * settled; if one of this call's drains failed, it then rejects with the first failure.
   * @public
   * @param {object} [options]
   * @param {number} [options.drainTimeout] ms each connection waits for its requests, 0 closing
   *   at once; each pool's own bound when omitted
   * @returns {Promise<void>} resolves when every broker has disconnected
   * @throws {Error} when a broker's disconnect fails
   */
  async disconnect(options) {
    if (!options?.preserveSetupState) {
      this.setupStates?.clear()
      this.setupMetadataEndpoints.clear()
    }
    this.retainSetupEndpoints()
    const brokers = new Set([...values(this.brokers), ...this.draining.keys()])
    if (this.seedBroker) brokers.add(this.seedBroker)
    this.disconnects++
    this.brokers = {}
    this.metadata = null
    this.installedMetadata = null
    this.versions = null
    this.authenticationBlackout.clear()

    const args = options ? [options] : []
    const drains = [...brokers].map((broker) => {
      const drain = (async () =>
        broker === this.seedBroker ? broker.disconnect(...args) : broker.retire(...args))()
      this.trackDrain(broker, drain)
      return drain
    })
    await Promise.all([...brokers].map((broker) => this.draining.get(broker)))
    const failure = (await Promise.allSettled(drains)).find(({ status }) => status === 'rejected')
    if (failure) throw failure.reason
  }

  /**
   * Removes a failed broker only when its identity and established pool generation are current.
   * An inherited baseline can name an old pre-write refusal, so it cannot authorize removal.
   * A sourced stale failure never falls back to address matching. Unsourced callers retain it.
   * @public
   * @param {{host: string, port: number, source?: {broker: object, generation: number}}} destination
   * @returns {void}
   */
  removeBroker({ host, port, source }) {
    const broker = source
      ? source.broker
      : values(this.brokers).find(
          (candidate) =>
            candidate.connectionPool.host === host && candidate.connectionPool.port === port
        )
    if (
      !broker ||
      !values(this.brokers).includes(broker) ||
      (source &&
        (broker.connectionPool.generation !== source.generation ||
          source.generation <= broker.connectionPool.generationBase))
    )
      return
    if (this.seedBroker === broker) {
      this.seedBroker = shuffle(values(this.brokers).filter((candidate) => candidate !== broker))[0]
    }
    this.metadataExpireAt = null
    const drain = this.closeObsolete(broker)
    if (drain) this.trackDrain(broker, drain)
  }

  /**
   * Fetches fresh cluster metadata and rebuilds the broker map from it.
   *
   * Retries transparently (re-throwing into the retrier) on `LEADER_NOT_AVAILABLE`, and on
   * `UNKNOWN_TOPIC_OR_PARTITION` when the broker allows auto topic creation — KRaft answers the
   * latter while a not-yet-created topic is still being created asynchronously through the
   * controller, so a bare retry is enough once the topic lands. Any other error bails out
   * immediately.
   *
   * An answer is installed in one synchronous step once the pools of the brokers it adds or moves
   * are built: the broker table, the setup endpoints, `this.metadata`, `metadataCache.update()`
   * (with its topic-recreated event), the `onMetadata` observer, the freshness and the routing
   * notification. Until then nothing of it is visible and the last installed answer stands, so no
   * reader or routing listener sees an answer whose brokers are not in place. A seed replaced
   * while the pools are built is taken into account at the install; an entry an endpoint hint
   * installed meanwhile is newer and kept.
   *
   * A cached broker whose host and port the response repeats keeps its object, and with it every
   * reference a caller holds; a new rack is written onto it in place. Only a moved broker is
   * rebuilt and the old one retired.
   *
   * Refreshes may overlap, and their replies may arrive in one read. Each accepted answer takes a
   * publication number, and only the latest one is installed: an older answer still building its
   * pools installs nothing, reports no success and closes its pools unused.
   *
   * `findConnectedBroker()` runs **inside** the retry callback and the seed is read at the
   * install: a rebootstrap partway through this method (trigger (c), fired from inside
   * `findConnectedBroker()`, or a concurrent direct `rebootstrap()` call) disconnects the broker
   * and rebuilds the seed, so re-resolving both is what keeps a later attempt from calling a
   * disconnected object or building a duplicate `Broker` for the seed's own node id from a stale
   * seed address. `findConnectedBroker()`'s own throw is `bail()`ed
   * immediately rather than left for the retrier to reclassify - the retrier's generic
   * catch-and-wrap would otherwise turn e.g. a `KafkaJSSASLAuthenticationError` into a
   * `KafkaJSNonRetriableError` and replace its `retryCount`/`retryTime`, breaking callers (the
   * consumer's own restart backoff among them) that pattern-match the original error shape.
   *
   * The whole method body is wrapped in one `try`/`catch` so failure is observed once per call,
   * not once per retrier iteration - this is what lets a `findConnectedBroker()` throw (nothing
   * reachable at all) start trigger (a)'s clock, and what lets trigger (b)'s `REBOOTSTRAP_REQUIRED`
   * (raised by `bail()` with the raw error) reach `onMetadataFailure` after the retrier settles.
   *
   * Each request belongs to the rebootstrap generation (`recoveries`) of the broker it was sent
   * to; a broker the selection's own rebootstrap (trigger (c)) returned belongs to the new one. An
   * answer or a failure that arrives after a rebootstrap belongs to the old cluster: nothing of it
   * is installed, it starts no recovery clock, and the call asks the rebuilt cluster once more,
   * within the same retry attempt, so `retries: 0` still gets that one request and the call's
   * retry count and deadline stay its own. A failure that caused the rebootstrap keeps its path.
   * When the call settles, the caller's guard, lifecycle and deadline come first, whatever the
   * outcome; then a rebootstrap after the last request rejects the call with a retriable
   * `KafkaJSConnectionError` and no recovery clock change; only an installed answer of the
   * current generation reports success. A refresh begun after a rebootstrap never joins one begun
   * before it.
   *
   * A consumer refresh whose subscription issued a newer refresh while it ran (and whose scope is
   * otherwise unchanged) hands over to that newer refresh if it is still in flight: it settles
   * with it, rejecting with its failure, and asks once more itself when that refresh installed
   * nothing or observed with another `onMetadata` observer. With nothing in flight it resolves, as
   * before.
   *
   * @public
   * @param {Array<String>} topics
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: once
   *   it closed, the retries stop, no socket opens, and a response in flight publishes nothing
   * @param {() => Error|null} [options.guard] checked before attempts and cache/routing installation
   * @param {object} [options.metadataScope] subscription that issues scope/sequence tickets
   * @param {() => boolean} [options.isCurrent] additional membership/target/lifecycle guard
   * @param {boolean} [options.allowAutoTopicCreation] per-call creation override
   * @param {boolean} [options.allowLeaderless] a partition without a leader is installed as it is
   *   (leader -1) instead of failing the refresh with `LEADER_NOT_AVAILABLE` until the retries end
   * @param {(metadata: object) => void} [options.onMetadata] synchronous accepted-snapshot observer
   * @returns {Promise<void>}
   * @throws {Error} the lifecycle's close error
   * @throws {Error} original refresh failure or caller refusal; expiry never triggers recovery work
   * @throws {KafkaJSConnectionError} retriable, when a rebootstrap superseded the call's request
   */
  async refreshMetadata(topics, options) {
    this.metadataDiscovery ??= shareMetadata(
      (names, requestOptions) => this.refreshMetadataUnshared(names, requestOptions),
      (names, requestOptions) =>
        requestOptions && Object.keys(requestOptions).length
          ? requestOptions
          : JSON.stringify([
              this.consumerMetadataGeneration ?? 0,
              this.consumerMetadataScope?.shareKey() ?? null,
              names || [],
            ]),
      () => this.disconnects,
      () => this.recoveries
    )
    return this.metadataDiscovery(topics, options)
  }

  /**
   * Refreshes one discovery episode with the original subscription and lifecycle checks.
   * @param {string[]} topics requested names
   * @param {object} [options] see `refreshMetadata`
   * @param {import('../consumer/subscription')} [options.metadataScope]
   * @param {() => boolean} [options.isCurrent]
   * @returns {Promise<void>} installs only a current snapshot
   * @throws {Error} as `refreshPass`
   */
  async refreshMetadataUnshared(topics, options = {}) {
    const { metadataScope = this.consumerMetadataScope, isCurrent = () => true } = options
    if (!isCurrent()) return
    await this.refreshPass(topics, options, metadataScope)
  }

  /**
   * One refresh: its requests, the install of its answer and its settlement, as `refreshMetadata`
   * describes them. A scoped refresh is the newest of its scope from its start. A refresh whose
   * answer newer work took over waits for that work within its own lifecycle and deadline, and
   * takes its outcome when it serves this call (see `takeOver` inside); otherwise it asks once
   * more itself, spending the call's one reissue, within the same retry attempt.
   * @param {string[]} topics requested names
   * @param {object} options see `refreshMetadata`
   * @param {import('../producer/generation')} [options.fence]
   * @param {() => Error|null} [options.guard]
   * @param {() => boolean} [options.isCurrent]
   * @param {boolean} [options.allowAutoTopicCreation]
   * @param {boolean} [options.allowLeaderless]
   * @param {(metadata: object) => void} [options.onMetadata]
   * @param {import('../consumer/subscription')|null|undefined} metadataScope the refresh's scope
   * @returns {Promise<void>}
   * @throws {Error} original refresh failure, caller refusal, or the failure of the newer refresh
   *   of its scope it was handed over to
   * @throws {KafkaJSConnectionError} retriable, when a rebootstrap or newer work superseded the
   *   call's request and its one reissue
   */
  async refreshPass(
    topics,
    { fence, guard, isCurrent = () => true, allowAutoTopicCreation, allowLeaderless, onMetadata },
    metadataScope
  ) {
    guard = withAdminGuard(guard)
    const check = () => {
      fence?.check()
      checked(null)
      const error = guard?.()
      if (error) throw error
    }
    const modeGeneration = this.consumerMetadataGeneration ?? 0
    const disconnects = this.disconnects
    let settlePass
    /** What an older refresh of the scope reads once this one settled; `failure` is a genuine one. */
    const pass = {
      sequence: 0,
      onMetadata,
      settled: false,
      done: new Promise((resolve) => (settlePass = resolve)),
      receipt: null,
      observed: false,
      failure: null,
    }
    let record = null
    let ticket = null
    /** Takes a new ticket of the scope and makes this refresh its newest. */
    const issue = () => {
      ticket = metadataScope.captureMetadata()
      pass.sequence = ticket.sequence
      record.newest = pass
    }
    // The call's scope still stands, though a newer refresh of that scope may have taken over.
    const live = () =>
      this.disconnects === disconnects &&
      !fence?.closed &&
      !guard?.() &&
      (this.consumerMetadataGeneration ?? 0) === modeGeneration &&
      isCurrent() &&
      (!ticket || ticket.isLive())
    const applicable = () => live() && (!ticket || ticket.isCurrent())
    const displaced = () => !!ticket && live() && !ticket.isCurrent()
    const clientPattern = metadataScope?.hasPatterns()
    if (clientPattern) topics = []
    if (clientPattern || this.consumerBrokerRegex) allowAutoTopicCreation = false
    const accept = this.captureTopics?.(topics || [])
    const establishment =
      this.unavailableTopics !== null && topics?.length > 0 ? this.captureEstablishment() : null
    const requestOptions = {
      ...guardArgs(fence)[0],
      ...(guard && { guard }),
      ...(allowAutoTopicCreation !== undefined && { allowAutoTopicCreation }),
      ...(allowLeaderless && { allowLeaderless }),
      ...((accept || establishment) && { retainTopicErrors: true }),
    }
    const requestArgs = Object.keys(requestOptions).length ? [requestOptions] : []
    /** The rebootstrap count the latest request belongs to. */
    let recovery = this.recoveries
    const current = () => this.recoveries === recovery
    const superseded = () =>
      new KafkaJSConnectionError('A rebootstrap replaced the metadata connection')
    /** The place in acceptance order of this call's latest answer. */
    let accepted = 0
    let observed = false
    /**
     * Waits for `promise` no longer than this call's lifecycle and deadline allow.
     * @param {Promise<unknown>} promise newer work's settlement, which never rejects
     * @returns {Promise<unknown>}
     * @throws {Error} the lifecycle's close error or the deadline's refusal
     */
    const bounded = (promise) => waitOf(() => (fence ? fence.wait(promise) : promise))

    /**
     * Judges the answer's topics against the targets as they stand now, with no effect.
     * @param {object} response the decoded Metadata answer
     * @returns {(notices: Array<() => void>) => object} applies the judgement inside the install
     *   step and returns the metadata to install
     * @throws {Error} a topic or partition error of the answer
     */
    const judge = (response) => {
      if (accept) {
        const { settled, drop } = this.settleTopicErrors(response.topicMetadata, accept)
        checkPartitions(settled, { allowLeaderless })
        const incoming = settled.filter(({ topic }) => accept(topic))
        return (notices) => {
          drop(notices)
          const replaced = new Set(incoming.map(({ topic }) => topic))
          const retained = (this.metadata?.topicMetadata || []).filter(
            ({ topic }) => !replaced.has(topic)
          )
          return { ...response, topicMetadata: [...retained, ...incoming] }
        }
      }
      if (establishment) {
        const { healthy, commit } = this.isolateUnknownTopics(
          response.topicMetadata,
          establishment,
          { allowLeaderless }
        )
        return (notices) => {
          commit(notices)
          return { ...response, topicMetadata: healthy }
        }
      }
      return () => response
    }

    /**
     * Builds the pools of the brokers the answer adds or moves, then installs it in the one
     * synchronous step (`installMetadata`), with its topics judged again against the targets as
     * they stand then. Only then do notices, the observer and the routing notification run. An
     * answer that a rebootstrap, the caller or a newer accepted answer overtook while its pools
     * were built installs nothing; pools the install did not take are closed on every path.
     * @param {object} response the decoded Metadata answer
     * @returns {Promise<MetadataReceipt|false|typeof SUPERSEDED|typeof OVERTAKEN>} the receipt;
     *   false when the caller's scope no longer applies
     * @throws {Error} a pool build, topic or observer failure; an observer runs after the install
     */
    const install = async (response) => {
      judge(response)
      const publication = ++this.metadataPublications
      accepted = publication
      let settleAnswer
      const answer = { publication, settled: false }
      answer.done = new Promise((resolve) => (settleAnswer = resolve))
      this.acceptedAnswer = answer
      const owns = () => applicable() && current() && this.metadataPublications === publication
      const previous = { ...this.brokers }
      const previousPools = new Map(
        Object.entries(previous).map(([nodeId, entry]) => [nodeId, entry.connectionPool])
      )
      // An entry, or a pool swapped in place, that an endpoint hint installed since is newer.
      const newer = (nodeId, table) => {
        const entry = table[nodeId]
        return (
          !!entry &&
          (entry !== previous[nodeId] || entry.connectionPool !== previousPools.get(String(nodeId)))
        )
      }
      const needsPool = (node) =>
        !newer(node.nodeId, this.brokers) &&
        !atAddress(this.brokers[node.nodeId], node) &&
        !atAddress(this.seedBroker, node)
      const pools = new Map()
      try {
        let missing
        while (
          owns() &&
          (missing = response.brokers.filter((node) => !pools.has(node.nodeId) && needsPool(node)))
            .length > 0
        ) {
          for (const { nodeId, host, port, rack } of missing) {
            const pool = await this.buildConnectionPool(
              { host, port, rack },
              { fence, isCurrent: owns }
            )
            if (!pool) break
            pools.set(nodeId, pool)
          }
        }
        if (!owns()) return !current() ? SUPERSEDED : applicable() ? OVERTAKEN : false

        const commit = judge(response)
        const notices = []
        // An empty topic list asks for every topic: the cache prunes what the answer lacks.
        const full = !accept && (!topics || topics.length === 0)
        const receipt = this.installMetadata(
          publication,
          { full, notices, isCurrent: owns },
          () => {
            this.installBrokers(response.brokers, { previous, newer, pools })
            return commit(notices)
          }
        )
        const metadata = this.metadata
        this.runNotices(notices)
        let thrown = null
        // A notice may have retired this call, or started a rebootstrap or a disconnect.
        if (onMetadata && applicable() && this.installedMetadata === receipt) {
          try {
            onMetadata(metadata)
            observed = true
          } catch (error) {
            thrown = error
          }
        }
        if (this.installedMetadata === receipt) this.metadataCache.publishRoutingChange()
        if (thrown) throw thrown
        return receipt
      } finally {
        for (const pool of pools.values()) this.closePool(pool)
        answer.settled = true
        settleAnswer()
      }
    }

    /**
     * Sends one Metadata request and installs its answer.
     * @param {(error: Error) => void} bail ends the call with `error`
     * @returns {Promise<MetadataReceipt|false|typeof SUPERSEDED|typeof OVERTAKEN|void>} see
     *   `install`; SUPERSEDED when a rebootstrap replaced the cluster the request went to;
     *   undefined once `bail` ended the call
     * @throws {Error} a failure the retrier retries
     */
    const request = async (bail) => {
      if (!applicable()) return false
      recovery = this.recoveries
      let broker
      try {
        check()
        broker = await this.findConnectedBroker(...(fence ? [{ fence }] : []))
        check()
      } catch (err) {
        // A failure that caused the rebootstrap (KIP-1242 on ApiVersions) keeps its own path.
        if (applicable() && !current() && !this.recoveredFailures.has(err)) return SUPERSEDED
        return bail(err)
      }
      if (!applicable()) return false
      if (!current()) {
        // Trigger (c) during selection returns a broker of the rebuilt cluster; a retired one is not.
        if (this.retiredBrokers.has(broker)) return SUPERSEDED
        recovery = this.recoveries
      }

      try {
        const response = await broker.metadata(topics, ...requestArgs)
        check()
        if (!applicable()) return false
        if (!current()) return SUPERSEDED
        return await install(response)
      } catch (e) {
        fence?.check()
        if (guard?.()) return bail(e)
        if (!applicable()) return false
        if (!current() && !this.recoveredFailures.has(e)) return SUPERSEDED
        if (
          e.type === 'LEADER_NOT_AVAILABLE' ||
          ((allowAutoTopicCreation ?? broker.allowAutoTopicCreation) &&
            e.type === 'UNKNOWN_TOPIC_OR_PARTITION')
        ) {
          throw e
        }

        bail(e)
      }
    }

    /**
     * Waits for the newer work that took this call's answer over: the newest refresh of its scope
     * when a newer one was issued, else the newest accepted answer. Its outcome serves this call
     * when it is installed and still held by the pool and, for a call with an observer, observed
     * by the same observer.
     * @returns {Promise<MetadataReceipt|null|false>} the receipt that serves the call; null when
     *   the call asks again itself; false when no newer refresh of its scope exists (another
     *   ticket of the scope took over) or the scope moved, so the call ends as before
     * @throws {Error} the call's refusal, or the failure of the newer refresh of its scope
     */
    const takeOver = async () => {
      if (displaced()) {
        let newest = record.newest
        while (newest !== pass && !newest.settled) {
          await bounded(newest.done)
          check()
          newest = record.newest
        }
        if (newest === pass || !live()) return false
        if (newest.failure) throw newest.failure
        const serves =
          !!newest.receipt &&
          newest.onMetadata === onMetadata &&
          (!onMetadata || newest.observed) &&
          this.holdsInstall(newest.receipt)
        return serves ? newest.receipt : null
      }
      let newest
      while ((newest = this.acceptedAnswer).publication > accepted && !newest.settled) {
        await bounded(newest.done)
        check()
      }
      const installed = this.installedMetadata
      return !onMetadata && installed && installed.publication > accepted ? installed : null
    }

    let reissued = false
    /** @type {MetadataReceipt|null} newer work's receipt this call took over */
    let adopted = null
    /** @type {Error|null} newer work's failure, or the refusal met while waiting for it */
    let adoptedFailure = null
    /**
     * One retrier attempt: a request, then, once per call, a second one when a rebootstrap
     * replaced the first one's cluster or newer work that took its answer over does not serve the
     * call. All of it runs inside the same attempt, so the call keeps its one retry count and
     * deadline, and `retries: 0` still gets the second request.
     * @param {(error: Error) => void} bail ends the call with `error`
     * @returns {Promise<MetadataReceipt|null>} the receipt of the answer that serves the call
     */
    const attempt = async (bail) => {
      for (;;) {
        const outcome = await request(bail)
        if (outcome === undefined) return null
        if (outcome === OVERTAKEN || (outcome === false && displaced())) {
          let taken
          try {
            taken = await takeOver()
          } catch (error) {
            adoptedFailure = error
            return bail(error)
          }
          if (taken) return (adopted = taken)
          if (taken === false) return null
        } else if (outcome !== SUPERSEDED) {
          return outcome || null
        }
        if (reissued) {
          return bail(
            outcome === SUPERSEDED
              ? superseded()
              : new KafkaJSConnectionError('A newer metadata refresh superseded this one')
          )
        }
        reissued = true
        if (ticket && !ticket.isCurrent()) issue()
      }
    }

    if (metadataScope) {
      record = this.scopeRefreshes.get(metadataScope) ?? { newest: pass, active: 0 }
      this.scopeRefreshes.set(metadataScope, record)
      record.active += 1
      issue()
    }
    try {
      let receipt = null
      let failure = null
      try {
        receipt = await this.retryFor(fence)(attempt)
      } catch (error) {
        failure = error
      }
      // The caller's refusal comes first, whatever the outcome.
      check()
      if (adopted || (failure && failure === adoptedFailure)) {
        if (failure) throw failure
        return
      }
      if (!applicable()) return
      // An outcome takes effect only on the cluster its request went to.
      if (!current() && !this.recoveredFailures.has(failure)) throw superseded()
      if (failure) {
        pass.failure = failure
        if (!fence?.closed) await this.onMetadataFailure(failure, { fence })
        throw failure
      }
      pass.receipt = receipt
      pass.observed = observed
      if (receipt && this.installedMetadata === receipt) this.onMetadataSuccess()
    } finally {
      pass.settled = true
      settlePass()
      if (record && --record.active === 0 && this.scopeRefreshes.get(metadataScope) === record) {
        this.scopeRefreshes.delete(metadataScope)
      }
    }
  }

  /**
   * The one install step of a metadata answer, synchronous and free of callbacks, so every
   * reader sees an answer whole or not at all. `apply` reconciles the writer's own state (broker
   * table, setup endpoints, topic targets) and returns the metadata to install; then
   * `this.metadata`, the cache and the freshness change, and a receipt names the install. Logs,
   * events and statuses go to `notices`. The caller then runs them (`runNotices`), calls its
   * observer, and wakes the routing listeners once while the receipt is still the installed one.
   * @private
   * @param {number} publication the answer's place in acceptance order
   * @param {object} options
   * @param {boolean} options.full whether the answer lists every topic
   * @param {Array<() => void>} options.notices receives the step's notifications
   * @param {() => boolean} options.isCurrent the install's ownership, checked by the caller
   * @param {() => object} apply the writer's own mutation
   * @returns {MetadataReceipt}
   */
  installMetadata(publication, { full, notices, isCurrent }, apply) {
    const metadata = apply()
    this.metadata = metadata
    this.metadataCache.update(metadata, { full, isCurrent, publish: false, notices })
    this.metadataExpireAt = Date.now() + this.metadataMaxAge
    this.installedMetadata = {
      publication,
      recovery: this.recoveries,
      disconnects: this.disconnects,
    }
    return this.installedMetadata
  }

  /**
   * Runs an install's notifications in order. One that throws is dropped, so application code
   * there can neither undo nor interrupt the install.
   * @private
   * @param {Array<() => void>} notices
   * @returns {void}
   */
  runNotices(notices) {
    for (const notice of notices) {
      try {
        notice()
      } catch {}
    }
  }

  /**
   * Whether the pool still holds an install: the same answer, or a newer one of the same
   * rebootstrap generation and connection.
   * @private
   * @param {MetadataReceipt} receipt
   * @returns {boolean}
   */
  holdsInstall(receipt) {
    const installed = this.installedMetadata
    return (
      !!installed &&
      installed.recovery === receipt.recovery &&
      installed.disconnects === receipt.disconnects &&
      installed.publication >= receipt.publication
    )
  }

  /**
   * Reconciles the broker table and setup endpoints with an answer's brokers, synchronously. A
   * cached broker the answer repeats keeps its object (a new rack is written in place), the seed
   * is adopted at its own address, a moved broker takes its prepared pool and the old one is
   * retired, and a broker the answer no longer lists is retired. An entry or pool installed after
   * the answer was accepted is newer: it is kept, with its own endpoint.
   * @private
   * @param {Array<{nodeId: number, host: string, port: number, rack: string|null}>} nodes
   * @param {object} prepared
   * @param {Record<string, Broker>} prepared.previous the table when the answer was accepted
   * @param {(nodeId: number, table: object) => boolean} prepared.newer
   * @param {Map<number, import('../network/connectionPool')>} prepared.pools built for the
   *   answer; the ones taken are removed
   * @returns {void}
   */
  installBrokers(nodes, { previous, newer, pools }) {
    const table = { ...this.brokers }
    const seed = this.seedBroker
    const seedAdvertised = nodes.some((node) => atAddress(seed, node))
    const kept = new Set()
    for (const node of nodes) {
      const { nodeId, rack } = node
      const entry = table[nodeId]
      if (newer(nodeId, table)) {
        kept.add(String(nodeId))
      } else if (atAddress(entry, node)) {
        entry.connectionPool.rack = rack
      } else if (atAddress(seed, node)) {
        seed.nodeId = nodeId
        this.bindTelemetryNode(seed)
        seed.connectionPool.rack = rack
        this.brokers[nodeId] = seed
        this.closeObsolete(entry)
      } else if (entry && entry === seed && !seedAdvertised) {
        this.replacePool(entry, pools.get(nodeId))
        pools.delete(nodeId)
      } else {
        this.brokers[nodeId] = this.createBroker({
          logger: this.rootLogger,
          versions: this.versions,
          connectionPool: pools.get(nodeId),
          nodeId,
        })
        pools.delete(nodeId)
        this.closeObsolete(entry)
      }
    }
    const listed = new Set(nodes.map(({ nodeId }) => String(nodeId)))
    for (const [nodeId, broker] of Object.entries(previous)) {
      if (!listed.has(nodeId) && this.brokers[nodeId] === broker) {
        if (broker === this.seedBroker) delete this.brokers[nodeId]
        else this.closeObsolete(broker)
      }
    }
    this.setupMetadataEndpoints = new Map(
      nodes.map((node) => {
        const { host, port } = kept.has(String(node.nodeId))
          ? table[node.nodeId].connectionPool
          : node
        return [String(node.nodeId), `${host}:${port}`]
      })
    )
    this.retainSetupEndpoints()
  }

  /**
   * Judges the topics a producer refresh got back with their own error code; the Java client keeps
   * such errors per topic too. An earlier target that is now unknown, invalid or unauthorized (a
   * deleted topic, say) never fails the refresh of the other targets, whether or not any metadata
   * is installed, as after a reconnect or a rebootstrap: it is dropped through `dropTopic`, and a
   * later send to it adds it again and fails on its own. One whose sends still own it is kept with
   * the metadata it had, if any, so they keep their routing and their retries ask about it again;
   * they recover once the broker serves it again, or end as any send does: with the broker's
   * answer, a routing error or the delivery timeout. A topic being added is not judged this way:
   * its refusal fails the call. A topic this refresh no longer captures is skipped. Any other
   * topic error is thrown, as the version's own parse would throw it, and nothing is dropped. The
   * caller checks the partitions of the returned topics only afterwards, so a topic error takes
   * precedence over a partition error, as in the strict parse. Judging changes nothing: the drops
   * happen in the install step of the answer, through `drop`, so an answer that is not installed
   * drops nothing, and they wake no routing listener of their own.
   * @param {object[]} topicMetadata the answer's topics, topic and partition errors retained
   * @param {(topic: string) => boolean} accept this refresh's publication predicate
   * @returns {{settled: object[], drop: (notices: Array<() => void>) => void}} the topics
   *   answered without an error, and the drops, which queue their log lines on `notices`
   * @throws {import('../errors').KafkaJSProtocolError} the first other topic error
   */
  settleTopicErrors(topicMetadata, accept) {
    const failed = topicMetadata.filter(
      ({ topicErrorCode }) => topicErrorCode != null && failure(topicErrorCode)
    )
    const drops = []
    let thrown = null
    for (const { topic, topicErrorCode } of failed) {
      if (!accept(topic)) continue
      const error = createErrorFromCode(topicErrorCode)
      if (!this.isNewTarget(topic) && DROPPED_TOPIC_ERRORS.has(error.type)) {
        drops.push({ topic, error })
      } else {
        thrown ??= error
      }
    }
    if (thrown) throw thrown
    return {
      settled: failed.length
        ? topicMetadata.filter((entry) => !failed.includes(entry))
        : topicMetadata,
      drop: (notices) => {
        for (const { topic, error } of drops) {
          const dropped = this.dropTopic(topic, { publish: false })
          notices.push(() =>
            this.logger.warn(
              dropped
                ? 'Dropping a target topic the broker no longer serves'
                : 'Keeping the metadata of a target topic its sends still own',
              { topic, error: error.message }
            )
          )
        }
      },
    }
  }

  /**
   * Judges the topics a consumer's named refresh got back with their own error code. Only
   * `UNKNOWN_TOPIC_OR_PARTITION` (code 3) for an established target, one a metadata answer
   * accepted since it was declared, is isolated: the topic is left out of the installed metadata,
   * so nothing routes to it, while it stays a target that every later refresh asks for again, and
   * its cached id stays, so a recreation is seen as one. Any other topic error, or code 3 for a
   * topic no answer accepted yet (a subscription whose validation failed, say), is thrown. Any other
   * code wins over code 3, whatever the wire order, so a missing topic never hides a fatal one (17,
   * 29); between two such codes the first in wire order wins. The healthy topics' partitions are
   * checked only then, so a topic error takes precedence over a partition error as in the strict
   * parse, and nothing changes before the whole answer passed. Judging changes nothing: `commit`
   * establishes the healthy topics and records the statuses inside the install step of the
   * answer, and queues the reports on its notices. A topic that is no longer a target (a
   * withdrawn first-add row, say) is left out first: it is neither judged, installed nor reported.
   * The Java consumer and franz-go keep a missing topic per topic too.
   *
   * A status is reported only when it changes: once when a topic becomes unavailable, once when a
   * refresh answers it again. The state is bounded by the targets; `forgetTopic` drops it. An
   * isolated topic is asked for again within a second (see `refreshMetadataIfNecessary`), so a
   * recreation is seen against its kept id and reset as any recreation is.
   * @param {object[]} answered the answer's topics, topic and partition errors retained
   * @param {TargetEstablishment} establishment captured when the refresh began
   * @param {{ allowLeaderless?: boolean }} options lets a partition without a leader through
   * @returns {{healthy: object[], commit: (notices: Array<() => void>) => void}} the topics
   *   answered without an error, and the step that establishes them and records the statuses
   * @throws {import('../errors').KafkaJSProtocolError} the first topic error other than code 3,
   *   else the first code 3 not isolated, else the first partition error
   */
  isolateUnknownTopics(answered, { isEstablished, isTarget, establish }, { allowLeaderless }) {
    const topicMetadata = answered.filter(({ topic }) => isTarget(topic))
    const failed = topicMetadata.filter(
      ({ topicErrorCode }) => topicErrorCode != null && failure(topicErrorCode)
    )
    const thrown =
      failed.find(({ topicErrorCode }) => topicErrorCode !== UNKNOWN_TOPIC_OR_PARTITION) ??
      failed.find(({ topic }) => !isEstablished(topic))
    if (thrown) throw createErrorFromCode(thrown.topicErrorCode)
    const healthy = failed.length
      ? topicMetadata.filter((entry) => !failed.includes(entry))
      : topicMetadata
    checkPartitions(healthy, { allowLeaderless })
    const commit = (notices) => {
      establish(healthy.map(({ topic }) => topic))
      this.isolatedRecheckAt = Date.now() + Math.min(this.metadataMaxAge, ISOLATED_RECHECK_MS)
      const report = (status) => notices.push(() => this.onTopicMetadataStatus(status))
      for (const { topic } of failed) {
        if (this.unavailableTopics.has(topic)) continue
        this.unavailableTopics.add(topic)
        report({
          topic,
          status: 'unavailable',
          type: 'UNKNOWN_TOPIC_OR_PARTITION',
          code: UNKNOWN_TOPIC_OR_PARTITION,
        })
      }
      for (const topic of this.unavailableTopics) {
        if (!healthy.some((entry) => entry.topic === topic)) continue
        this.unavailableTopics.delete(topic)
        report({ topic, status: 'available', type: null, code: 0 })
      }
    }
    return { healthy, commit }
  }

  /**
   * Drops one target from the retained decoded response, preserving broker discovery, and from
   * the isolated targets.
   * @param {string} topic forgotten name
   * @returns {void}
   */
  forgetTopic(topic) {
    this.unavailableTopics?.delete(topic)
    if (this.metadata) {
      this.metadata = {
        ...this.metadata,
        topicMetadata: this.metadata.topicMetadata.filter((entry) => entry.topic !== topic),
      }
    }
  }

  /**
   * Called after a metadata refresh completes. A response with at least one broker is what
   * KIP-1102 counts as availability returning, so it clears trigger (a)'s continuous-failure
   * clock and re-arms trigger (c)'s one-shot.
   * @private
   * @returns {void}
   */
  onMetadataSuccess() {
    if (!this.metadata || this.metadata.brokers.length === 0) {
      return
    }

    this.metadataUnavailableSince = null
    this.rebootstrapArmed = true
  }

  /**
   * Called when a metadata refresh call fails, whatever the cause - a broker error, an exhausted
   * retrier, or `findConnectedBroker()` finding nothing reachable at all. One call is one
   * attempt: the retrier's own iterations inside it are not counted separately, because the call
   * is the unit KIP-1102's "metadata request" maps onto.
   *
   * @private
   * @param {Error} e
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle whose refresh
   *   failed, carried into any rebootstrap it starts
   * @returns {Promise<void>}
   */
  async onMetadataFailure(e, { fence } = {}) {
    if (this.metadataRecoveryStrategy !== 'rebootstrap' || this.recoveredFailures.has(e)) {
      return
    }

    // Trigger (b): the broker told us outright. Deliberately not rate-limited, and it shares
    // rebootstrap()'s promise with any concurrent caller. This is unbounded and it does amplify:
    // `rebootstrap()` nulls `this.metadata`, so `refreshMetadataIfNecessary` always refreshes
    // again next call - a broker that persistently answers 129 produces one full teardown and one
    // REBOOTSTRAP event per caller iteration (e.g. per consumer fetch), throttled only by the
    // caller's own retry/backoff.
    if (e.type === 'REBOOTSTRAP_REQUIRED') {
      await this.rebootstrap('required', { fence })
      return
    }

    // A SASL authentication failure is never a rebootstrap trigger. A rejected credential means
    // the broker is fine and reachable - only this client is at fault, which no rebootstrap can
    // fix - so it must neither start nor advance trigger (a)'s clock here. `return` leaves
    // `metadataUnavailableSince` exactly as it was (unset stays unset; a clock already running
    // from an earlier, non-auth failure keeps running unmodified).
    if (isAuthenticationFailure(e)) {
      return
    }

    // Trigger (a): the continuous-failure clock, and nothing else. It is the only gate this
    // trigger has: `rebootstrap()` clears the clock, so the next failure starts a fresh window
    // and a client that keeps failing rebootstraps once per window, for as long as it keeps
    // failing - which is KIP-1102's behaviour. Gating it on `rebootstrapArmed` as well would cap
    // an unreachable cluster at exactly one rebootstrap, ever.
    const now = Date.now()

    if (this.metadataUnavailableSince == null) {
      this.metadataUnavailableSince = now
      return
    }

    if (now - this.metadataUnavailableSince >= this.metadataRecoveryRebootstrapTriggerMs) {
      await this.rebootstrap('timeout', { fence })
    }
  }

  /**
   * The one owner of a REBOOTSTRAP_REQUIRED answer to an ApiVersions request (KIP-1242), whether the
   * request was a connection's negotiation or a fresh `apiVersions()` read. It runs the same
   * recovery as a metadata failure of that kind, once per error: the error is then thrown on to
   * its caller, and a metadata or connect catch that meets it again does nothing. Recovery is
   * skipped when the broker no longer owns the pool the request went out on, or is no longer in
   * this pool's tables, so an answer that outlived its broker never touches the replacement.
   *
   * A recovery that fails is logged and never rejects: the caller is already failing with the
   * answer itself, which is the error it should see.
   *
   * A request made for a producer lifecycle (`guard`) is recovered under it, like a fenced metadata
   * refresh: a lifecycle that closed, now or while the seed resolves, stops the recovery before it
   * replaces the seed or publishes anything.
   *
   * @private
   * @param {Error} error the typed REBOOTSTRAP_REQUIRED error
   * @param {{broker: Broker, pool: import("../network/connectionPool"), guard?: () => Error|null}} source
   *   the broker that sent the request, the pool it was sent on, and the lifecycle check the
   *   request was made under, if any
   * @returns {Promise<void>}
   */
  async onApiVersionsFailure(error, { broker, pool, guard }) {
    const current =
      !broker.retired &&
      broker.connectionPool === pool &&
      (broker === this.seedBroker || values(this.brokers).includes(broker))
    if (!current) {
      return
    }

    try {
      await this.onMetadataFailure(error, { fence: guard && fenceOf(guard) })
      this.recoveredFailures.add(error)
    } catch (recoveryError) {
      try {
        this.logger.warn('Rebootstrap after an ApiVersions answer failed', {
          error: describeThrown(recoveryError),
        })
      } catch {}
    }
  }

  /**
   * How the cached metadata stands for `topics`, as `refreshMetadataIfNecessary` judges it:
   * `missing` without an answer or when a topic is not in it (an isolated consumer target counts as
   * in it, see there), else `aged` past `metadataMaxAge`, else `fresh`.
   * @param {string[]} topics
   * @param {object} [metadataScope] a pattern scope judges no topics
   * @returns {'missing'|'aged'|'fresh'}
   */
  metadataState(topics, metadataScope = this.consumerMetadataScope) {
    if (this.metadata == null || this.metadataExpireAt == null) return 'missing'
    const now = Date.now()
    const known = (topic) =>
      (this.unavailableTopics?.has(topic) && now <= this.isolatedRecheckAt) ||
      BrokerPool.topicRowsByName(this.metadata).has(topic)
    if (!metadataScope?.hasPatterns() && !topics.every(known)) return 'missing'
    return now > this.metadataExpireAt ? 'aged' : 'fresh'
  }

  /**
   * Only refreshes metadata if the data is stale according to the `metadataMaxAge` param or does not contain information about the provided topics
   * An isolated consumer target counts as known for at most a second after the refresh that
   * isolated it (or `metadataMaxAge`, when shorter), so a recreated topic is found again within
   * that second, as the Java consumer keeps asking for a partition without a leader, while a fetch
   * loop that calls this on every round does not ask for it on every round.
   *
   * @public
   * @param {Array<String>} topics
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] see `refreshMetadata`
   * @returns {Promise<null>}
   */
  async refreshMetadataIfNecessary(topics, options = {}) {
    const metadataScope = options.metadataScope ?? this.consumerMetadataScope
    if (metadataScope?.hasPatterns()) topics = []
    if (this.metadataState(topics, metadataScope) !== 'fresh') {
      const context = metadataScope ? { ...options, metadataScope } : options
      return Object.keys(context).length
        ? this.refreshMetadata(topics, context)
        : this.refreshMetadata(topics)
    }
  }

  /**
   * An answer's rows by topic name; of two rows with one name, the first wins, as a search of the
   * array would find it. Cached per `topicMetadata` array: every writer of `metadata` installs a
   * new array and none renames, adds or removes rows in place, so a new array is the one
   * invalidation point. Held weakly by that array, so it lives as long as the answer it indexes.
   * @param {{ topicMetadata?: object[] } | null} metadata
   * @returns {Map<string, object>} empty for no answer
   */
  static topicRowsByName(metadata) {
    const rows = metadata?.topicMetadata
    if (!rows) return new Map()
    let byName = topicRowIndexes.get(rows)
    if (!byName) {
      byName = new Map()
      for (const row of rows) {
        const { topic } = row
        if (!byName.has(topic)) byName.set(topic, row)
      }
      topicRowIndexes.set(rows, byName)
    }
    return byName
  }

  /** @type {() => string[]} */
  getNodeIds() {
    return keys(this.brokers)
  }

  /**
   * Whether this pool currently holds a broker for the given node id.
   *
   * The broker map is keyed by `Object.keys`, i.e. **strings**, while every node id decoded off
   * the wire is an `int32` **number**. This is the one place that boundary is crossed, so callers
   * never have to know which side they are on.
   *
   * @public
   * @param {number|string} nodeId
   * @returns {boolean}
   */
  hasNode(nodeId) {
    return String(nodeId) in this.brokers
  }

  /**
   * @public
   * @param {object} options
   * @param {string} options.nodeId
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: its
   *   close ends the connection attempts, and a closed one is refused
   * @returns {Promise<Broker>}
   * @throws {Error} the lifecycle's close error
   * @throws {KafkaJSConnectionError} retriable, with code `SELECTION_SUPERSEDED`, when the broker
   *   was retired, its pool swapped or the pool disconnected while it connected
   */
  async findBroker({ nodeId, fence }) {
    fence?.check()
    checked(null)
    const disconnects = this.disconnects
    const broker = this.brokers[nodeId]

    if (!broker) {
      throw new KafkaJSBrokerNotFound(`Broker ${nodeId} not found in the cached metadata`)
    }

    const pool = await this.connectBroker(broker, ...(fence ? [{ fence }] : []))
    fence?.check()
    checked(null)
    return this.owned(broker, disconnects, pool)
  }

  /** @returns {string} the `code` of the error a superseded broker selection fails with */
  static get SELECTION_SUPERSEDED() {
    return SELECTION_SUPERSEDED
  }

  /**
   * Hands a selected broker on only while the pool still holds it as it was selected: not
   * retired (by a rebootstrap, a replacement or a disconnect), still on the connection pool that
   * connected, and no disconnect since the selection began. So no caller gets a broker the pool
   * gave up, or one whose connected pool was swapped for a cold one.
   * @private
   * @param {Broker} broker the selected broker
   * @param {number} disconnects the disconnect count when the selection began
   * @param {import('../network/connectionPool')} [pool] the pool that connected; the attached one
   *   when the selection did not connect it
   * @returns {Broker} the same broker
   * @throws {KafkaJSConnectionError} retriable, with code `SELECTION_SUPERSEDED`
   */
  owned(broker, disconnects, pool = broker.connectionPool) {
    if (
      this.retiredBrokers.has(broker) ||
      broker.retired ||
      broker.connectionPool !== pool ||
      this.disconnects !== disconnects
    ) {
      throw selectionSuperseded()
    }
    return broker
  }

  /**
   * Tries brokers in shuffled order, skipping connection failures as well as callback failures.
   * Other lookup failures propagate immediately. If no callback succeeds, the last connection
   * failure is thrown if any broker failed to connect; otherwise returns null.
   * @public
   * @param {(params: { nodeId: string, broker: Broker }) => Promise<T>} callback
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: once
   *   it closed, no further broker is tried
   * @returns {Promise<T|null>}
   * @throws {Error} a lookup failure, the last connection failure or the lifecycle's close error
   * @template T
   */
  async withBroker(callback, { fence } = {}) {
    const brokers = shuffle(keys(this.brokers))
    if (brokers.length === 0) {
      throw new KafkaJSBrokerNotFound('No brokers in the broker pool')
    }

    let connectionError = null
    for (const nodeId of brokers) {
      let broker
      try {
        broker = await this.findBroker({ nodeId, fence })
      } catch (e) {
        fence?.check()
        checked(null)
        if (!isConnectionFailure(e)) throw e
        connectionError = e
        continue
      }
      try {
        return await callback({ nodeId, broker })
      } catch (e) {
        fence?.check()
        checked(null)
      }
    }

    if (connectionError) throw connectionError
    return null
  }

  /**
   * @public
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: once
   *   it closed, no broker is tried, no socket opens, and a rebootstrap it started installs no pool
   * @returns {Promise<Broker>} a broker the pool still holds: a broker of a scan that a
   *   rebootstrap overtook is not returned, and that rebootstrap's seed is connected instead;
   *   when the seed fallback finds a cached broker connected already, that broker
   * @throws {Error} the lifecycle's close error
   * @throws {KafkaJSConnectionError} retriable, with code `SELECTION_SUPERSEDED`, when the broker
   *   it connected was retired or replaced, or the pool disconnected, before it returned
   */
  async findConnectedBroker({ fence } = {}) {
    fence?.check()
    checked(null)
    const disconnects = this.disconnects
    const recovery = this.recoveries
    const nodeIds = shuffle(keys(this.brokers))
    const connectedBrokerId = nodeIds.find((nodeId) => this.brokers[nodeId].isConnected())

    if (connectedBrokerId) {
      return await this.findBroker({ nodeId: connectedBrokerId, fence })
    }

    // Cycle through the nodes until one connects. Tracked alongside: whether every failure seen
    // was a SASL authentication failure - trigger (c) below must never fire off the back of
    // blacked-out credentials alone (the cluster is reachable; only this client's credentials are
    // bad), so a single non-auth failure is what allows it to fire at all.
    let allFailuresWereAuthentication = nodeIds.length > 0
    for (const nodeId of nodeIds) {
      fence?.check()
      checked(null)
      // A rebootstrap replaced the table these ids came from.
      if (this.recoveries !== recovery) break
      try {
        return await this.findBroker({ nodeId, fence })
      } catch (e) {
        fence?.check()
        checked(null)
        if (!isAuthenticationFailure(e)) {
          allFailuresWereAuthentication = false
        }
      }
    }

    // Every cached broker failed to connect. Under the rebootstrap strategy this is KIP-899's
    // "all brokers unavailable": rebuild the seed from the configured list instead of retrying
    // on top of metadata we now have every reason to distrust. Bounded three ways - it needs a
    // non-empty broker table (an empty one makes "every broker failed" vacuously true, which is
    // exactly the state a rebootstrap leaves behind), a successful metadata response since the
    // last rebootstrap (which is what stops one unreachable cluster producing a teardown per
    // retrier iteration), and at least one failure that was not purely an authentication failure.
    // A scan that a rebootstrap overtook judges nothing: that rebootstrap already rebuilt the seed.
    fence?.check()
    checked(null)
    if (
      this.recoveries === recovery &&
      this.shouldRebootstrapOnAllBrokersUnavailable(nodeIds, allFailuresWereAuthentication)
    ) {
      await this.rebootstrap('all-brokers-unavailable', { fence })
      fence?.check()
      checked(null)
    }

    // Failed to connect to all known brokers, metadata might be old
    const seedRecovery = this.recoveries
    await this.connect(...(fence ? [{ fence }] : []))
    fence?.check()
    checked(null)
    // A rebootstrap after the connect replaced the seed it connected with a cold one.
    if (this.recoveries !== seedRecovery) throw selectionSuperseded()
    // `connect` returns at once when a cached broker is connected already: that one is selected.
    const connected = values(this.brokers).find((broker) => broker.isConnected())
    return this.owned(connected ?? this.seedBroker, disconnects)
  }

  /**
   * @private
   * @param {Broker} broker
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] see `findBroker`
   * @returns {Promise<import('../network/connectionPool')>} the pool that connected
   * @throws {KafkaJSConnectionError} retriable, with code `SELECTION_SUPERSEDED`, when the pool
   *   retired the broker while it connected or cleaned up; a retired broker is not repaired
   */
  async connectBroker(broker, { fence } = {}) {
    if (broker.isConnected()) {
      return broker.connectionPool
    }

    // A broker the pool retired is draining already: it is not repaired, and its cleanup stops.
    const retired = () => this.retiredBrokers.has(broker) || broker.retired
    return this.retryFor(fence)(async (bail, retryCount, retryTime) => {
      const pool = broker.connectionPool
      try {
        fence?.check()
        checked(null)
        await broker.connect(...guardArgs(fence))
        fence?.check()
        checked(null)
        return pool
      } catch (caught) {
        fence?.check()
        checked(null)
        const e = isNativeError(caught)
          ? caught
          : new KafkaJSNonRetriableError(describeThrown(caught), { cause: caught })
        if (retired()) return bail(selectionSuperseded(e))
        if (isConnectionFailure(e) || describeThrown(e, 'type') === 'ILLEGAL_SASL_STATE') {
          await broker.disconnect()
          if (retired()) return bail(selectionSuperseded(e))
        }

        // To avoid reconnecting to an unavailable host, we bail on connection errors
        // and refresh metadata on a higher level before reconnecting
        if (isConnectionFailure(e)) {
          return bail(e)
        }

        if (describeThrown(e, 'type') === 'ILLEGAL_SASL_STATE') {
          // Rebuild the connection pool since it can't recover from illegal SASL state.
          // replaceConnectionPool also resets versionsGeneration, so the next connect() always
          // re-negotiates against the replacement pool (KIP-35 rule 7) rather than comparing
          // against the generation the old, discarded pool had reached.
          const previousPool = broker.connectionPool
          const isCurrent = () => broker.connectionPool === previousPool && !retired()
          const replacement = await this.buildConnectionPool(
            { host: previousPool.host, port: previousPool.port, rack: previousPool.rack },
            { fence, isCurrent }
          )
          if (!replacement) return bail(selectionSuperseded(e))
          if (fence?.closed || currentDeadline()?.guard()) {
            this.closePool(replacement)
            fence?.check()
            checked(null)
          }
          if (!isCurrent()) {
            this.closePool(replacement)
            return bail(selectionSuperseded(e))
          }
          this.replacePool(broker, replacement)

          try {
            this.logger.error(`Failed to connect to broker, reconnecting`, {
              retryCount,
              retryTime,
            })
          } catch {}
          const failure = new KafkaJSProtocolError({
            type: 'ILLEGAL_SASL_STATE',
            code: 34,
            message: describeThrown(e),
            retriable: true,
          })
          failure.cause = e
          throw failure
        }

        if (describeThrown(e, 'retriable') === 'true') throw e
        try {
          this.logger.error(e, {
            retryCount,
            retryTime,
            stack: describeThrown(e, 'stack'),
          })
        } catch {}
        bail(e)
      }
    })
  }

  /**
   * Gate for trigger (c) (KIP-899's "all brokers unavailable"): true only when the strategy
   * allows it, there was actually a broker table to exhaust, a success since the last rebootstrap
   * armed this cycle, and at least one of the failures was not purely a SASL authentication
   * failure - a cluster that is fully reachable but rejecting this client's credentials is not
   * "unavailable", and no rebootstrap can fix a bad credential.
   * @private
   * @param {string[]} nodeIds the cached broker ids that were all just tried
   * @param {boolean} allFailuresWereAuthentication true when every failed attempt above was a
   *        SASL authentication failure
   * @returns {boolean}
   */
  shouldRebootstrapOnAllBrokersUnavailable(nodeIds, allFailuresWereAuthentication) {
    return (
      this.metadataRecoveryStrategy === 'rebootstrap' &&
      nodeIds.length > 0 &&
      this.rebootstrapArmed &&
      !allFailuresWereAuthentication
    )
  }

  /**
   * Emits a cluster-level instrumentation event, best-effort, exactly as `MetadataCache#emit`
   * does. Listeners are invoked synchronously by the emitter and the clients' public `on()`
   * wrapper only guards a rejected promise, so an application listener that throws synchronously
   * would otherwise reject the shared `rebootstrap()` promise - and with it the metadata refresh
   * that triggered the rebootstrap, replacing the caller's original error with the listener's.
   * A throw is logged and dropped instead. Listener work does not belong to the publishing
   * discovery's lifetime.
   *
   * @private
   * @param {string} eventName
   * @param {object} payload
   * @returns {void}
   */
  emit(eventName, payload) {
    if (!this.instrumentationEmitter) {
      return
    }

    try {
      sharedSetup(() => this.instrumentationEmitter.emit(eventName, payload))
    } catch (e) {
      this.logger.error('Instrumentation event listener threw, ignoring it', {
        eventName,
        error: e.message,
      })
    }
  }
}
