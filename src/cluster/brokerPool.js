const {
  currentDeadline,
  checked,
  shareMetadata,
  retrierOf,
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

const { keys, assign, values } = Object
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

const PRIVATE = {
  REBOOTSTRAP: Symbol('private:BrokerPool:rebootstrap'),
}

module.exports = class BrokerPool {
  /**
   * Owns broker readiness and retained routing responses; optional producer captures fence topic merges.
   * @param {object} options
   * @param {import("./connectionPoolBuilder").ConnectionPoolBuilder} options.connectionPoolBuilder
   * @param {import("../../types").Logger} options.logger
   * @param {import("./metadataCache")} [options.metadataCache] optional: a `BrokerPool` owns its
   *   metadata and is usable standalone, so it defaults to a private cache rather than requiring
   *   a collaborator it can build itself. `Cluster` always injects its own, which is then the
   *   single instance every route reads.
   * @param {(topics: string[]) => ((topic: string) => boolean)} [options.captureTopics] scoped producer publication predicate
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
   * @param {number} [options.metadataRecoveryRebootstrapTriggerMs=300000] how long metadata must
   *   fail *continuously* (trigger (a)) before a rebootstrap fires.
   */
  constructor({
    connectionPoolBuilder,
    logger,
    metadataCache,
    captureTopics,
    instrumentationEmitter,
    retry,
    allowAutoTopicCreation,
    authenticationTimeout,
    metadataMaxAge,
    metadataRecoveryStrategy,
    metadataRecoveryRebootstrapTriggerMs,
    drainTimeout,
  }) {
    this.captureTopics = captureTopics
    this.drainTimeout = drainTimeout
    this.retiredBrokers = new WeakSet()
    this.rootLogger = logger
    this.connectionPoolBuilder = connectionPoolBuilder
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

    /**
     * Every broker of this pool is built here, so each one names the identity the pool currently
     * holds in its ApiVersions requests, follows the pool's recovery strategy and reports a
     * REBOOTSTRAP_REQUIRED answer to `onApiVersionsFailure`.
     * @param {object} options the `Broker` options the call site chooses
     * @returns {Broker}
     */
    this.createBroker = (options) => {
      const broker = new Broker({
        allowAutoTopicCreation,
        authenticationTimeout,
        metadataRecoveryStrategy: this.metadataRecoveryStrategy,
        metadataIdentity: () => ({
          clusterId: this.metadata?.clusterId ?? null,
          nodeId: broker.nodeId ?? -1,
        }),
        onApiVersionsFailure: (error, source) => this.onApiVersionsFailure(error, source),
        ...options,
      })
      return broker
    }

    this.brokers = {}
    /** @type {Broker | undefined} */
    this.seedBroker = undefined
    /** @type {import("../../types").BrokerMetadata | null} */
    this.metadata = null
    this.metadataExpireAt = null
    /** Counts the responses published to `this.metadata`, so a refresh can tell it was superseded. */
    this.metadataPublications = 0
    /** @type {import('../consumer/subscription')|null} default scope of a client-pattern consumer */
    this.consumerMetadataScope = null
    /** Advances when the default scope is registered or released, fencing earlier unscoped work. */
    this.consumerMetadataGeneration = 0
    this.versions = null

    this.metadataRecoveryStrategy = metadataRecoveryStrategy ?? 'rebootstrap'
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
        const publication = this.metadataPublications
        const brokers = new Set([seed, ...values(table)])
        const ownsRouting = () =>
          this.brokers === table && this.metadataPublications === publication
        const installed = await this.createSeedBroker({
          fence,
          isCurrent: () => ownsRouting() && this.seedBroker === seed,
        })
        fence?.check()
        checked(null)
        if (!ownsRouting() || this.seedBroker !== installed) {
          throw new KafkaJSConnectionError('Recovery routing was superseded')
        }
        this.metadata = null
        this.metadataExpireAt = null
        this.metadataCache.clear()
        for (const broker of brokers) {
          if (broker !== seed && broker !== installed) this.closeObsolete(broker)
        }
        this.lastRebootstrapAt = Date.now()
        this.metadataUnavailableSince = null
        this.rebootstrapArmed = false
        this.logger.warn('Rebootstrapping the client from the configured broker list', { reason })
        this.emit(CLUSTER_REBOOTSTRAP, { reason })
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
   * A replacement seed is installed before the captured old identities are detached and
   * retired. Aliases are deduplicated and replacements installed during the build survive.
   * Draining proceeds in the background under the configured obsolete-resource bound.
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
   * @returns {void} never waits for a drain
   */
  closeObsolete(broker) {
    if (!broker || broker === this.seedBroker) return
    for (const nodeId of keys(this.brokers)) {
      if (this.brokers[nodeId] === broker) delete this.brokers[nodeId]
    }
    if (this.retiredBrokers.has(broker)) return
    this.retiredBrokers.add(broker)
    this.closeInBackground(broker.retire({ drainTimeout: this.drainTimeout }))
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
    if (broker.replaceConnectionPool(pool)) this.closePool(previous)
  }

  /**
   * Fences a replaced or never-installed pool, then drains it in the background.
   * @private
   * @param {import('../network/connectionPool')} pool obsolete pool
   * @returns {void}
   */
  closePool(pool) {
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
   * @returns {Promise<Broker>} installed seed
   * @throws {Error} original caller refusal or superseded recovery identity
   */
  async createSeedBroker({ fence, isCurrent = () => true } = {}) {
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
    return this.seedBroker
  }

  /**
   * Connects the seed broker, retrying and rotating through the configured broker list.
   *
   * The seed is re-read *inside* the retry callback: a rebootstrap or a `removeBroker()` can
   * replace (or, for `removeBroker`, unset) `this.seedBroker` between two scheduled attempts, so
   * a reference captured before the retrier would connect an object nothing else uses. A missing
   * seed is reported as a retriable connection error rather than dereferenced - the retrier
   * treats a `TypeError` as unrecoverable and would turn a transient window into a crash.
   *
   * @public
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: its
   *   close ends the retries and opens no socket afterwards
   * @returns {Promise<void>}
   * @throws {Error} the lifecycle's close error
   */
  async connect({ fence } = {}) {
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
        this.versions = seedBroker.versions
      } catch (caught) {
        fence?.check()
        checked(null)
        const e = isNativeError(caught)
          ? caught
          : new KafkaJSNonRetriableError(describeThrown(caught), { cause: caught })
        if (isConnectionFailure(e) || describeThrown(e, 'type') === 'ILLEGAL_SASL_STATE') {
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
   * Disconnects the seed and every broker in the table once, starting all drains concurrently.
   * Detaches the captured table before draining, and retires its non-seed brokers so a stale
   * lookup cannot reconnect an orphan. The seed stays reusable and owned. Work resumed during
   * the drain builds a new table, which this call leaves intact. If one drain fails, this call
   * rejects while the other drains continue; the captured table stays detached.
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
    const brokers = new Set(values(this.brokers))
    if (this.seedBroker) brokers.add(this.seedBroker)
    this.brokers = {}
    this.metadata = null
    this.versions = null
    this.authenticationBlackout.clear()

    await Promise.all(
      [...brokers].map((broker) =>
        broker === this.seedBroker
          ? broker.disconnect(...(options ? [options] : []))
          : broker.retire(...(options ? [options] : []))
      )
    )
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
    this.closeObsolete(broker)
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
   * Feeds the fresh response to `metadataCache.update()` synchronously, right after the
   * `this.metadata` assignment, so every route reading cached leader/epoch data sees a
   * consistent view before anything else in this method runs.
   *
   * A cached broker whose host and port the response repeats keeps its object, and with it every
   * reference a caller holds; a new rack is written onto it in place. Only a moved broker is
   * rebuilt and the old one retired.
   *
   * Refreshes may overlap, and their replies may arrive in one read. Each response takes a
   * publication number when it is assigned to `this.metadata`, and a refresh stops reconciling the
   * broker table as soon as a newer response has been published: the topology last published is
   * the one installed, and a pool built for an older one is closed unused.
   *
   * `findConnectedBroker()` and the seed host/port read happen **inside** the retry callback:
   * a rebootstrap partway through this method (trigger (c), fired from inside
   * `findConnectedBroker()`, or a concurrent direct `rebootstrap()` call) disconnects the broker
   * and rebuilds the seed, so re-resolving both on every iteration is what keeps a later attempt
   * from calling a disconnected object or building a duplicate `Broker` for the seed's own node
   * id via a stale `seedHost`/`seedPort` pair. `findConnectedBroker()`'s own throw is `bail()`ed
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
            ])
    )
    return this.metadataDiscovery(topics, options)
  }

  /**
   * Refreshes one discovery episode with the original subscription and lifecycle checks.
   * @param {string[]} topics requested names
   * @param {object} [options] original routing policy
   * @returns {Promise<void>} installs only a current snapshot
   * @throws {Error} original routing or episode refusal
   */
  async refreshMetadataUnshared(
    topics,
    {
      fence,
      guard,
      metadataScope = this.consumerMetadataScope,
      isCurrent = () => true,
      allowAutoTopicCreation,
      allowLeaderless,
      onMetadata,
    } = {}
  ) {
    guard = withAdminGuard(guard)
    const check = () => {
      fence?.check()
      checked(null)
      const error = guard?.()
      if (error) throw error
    }
    if (!isCurrent()) return
    const modeGeneration = this.consumerMetadataGeneration ?? 0
    const ticket = metadataScope?.captureMetadata()
    const applicable = () =>
      !fence?.closed &&
      !guard?.() &&
      (this.consumerMetadataGeneration ?? 0) === modeGeneration &&
      isCurrent() &&
      (!ticket || ticket.isCurrent())
    if (metadataScope?.hasPatterns()) {
      topics = []
      allowAutoTopicCreation = false
    }
    const accept = this.captureTopics?.(topics || [])
    const requestOptions = {
      ...guardArgs(fence)[0],
      ...(guard && { guard }),
      ...(allowAutoTopicCreation !== undefined && { allowAutoTopicCreation }),
      ...(allowLeaderless && { allowLeaderless }),
    }
    const requestArgs = Object.keys(requestOptions).length ? [requestOptions] : []
    try {
      await this.retryFor(fence)(async (bail, retryCount, retryTime) => {
        if (!applicable()) return
        let broker
        try {
          check()
          broker = await this.findConnectedBroker(...(fence ? [{ fence }] : []))
          check()
        } catch (err) {
          return bail(err)
        }
        fence?.check()
        checked(null)

        // Captured once, before any `await` in this callback: a concurrent rebootstrap (reachable
        // because `Cluster#metadata()` calls `refreshMetadataIfNecessary` directly, bypassing
        // both of `Cluster`'s shared-promise guards) swaps `this.seedBroker` for a fresh instance
        // partway through, and re-reading it after the `await` below would mix the two seeds'
        // state within one attempt.
        if (!applicable()) return
        const seedBroker = this.seedBroker
        const { host: seedHost, port: seedPort } = seedBroker ? seedBroker.connectionPool : {}

        try {
          const response = await broker.metadata(topics, ...requestArgs)
          fence?.check()
          check()
          if (!applicable()) return
          if (accept) {
            const incoming = response.topicMetadata.filter(({ topic }) => accept(topic))
            const replaced = new Set(incoming.map(({ topic }) => topic))
            const retained = (this.metadata?.topicMetadata || []).filter(
              ({ topic }) => !replaced.has(topic)
            )
            this.metadata = { ...response, topicMetadata: [...retained, ...incoming] }
          } else {
            this.metadata = response
          }
          // Captured synchronously, in the same statement-tick as the assignment above and
          // before anything below can `await` - a concurrent rebootstrap nulls `this.metadata`
          // as soon as it starts (see the `seedBroker` capture above for why this concurrency is
          // reachable), and the reduce below awaits once per broker. Reading `this.metadata`
          // again after those awaits - as this used to - risks a `TypeError` the retrier treats
          // as unrecoverable, which would both escape to the caller undiagnosed and (wrongly)
          // start trigger (a)'s clock over an internal race rather than real broker
          // unavailability. Everything below reads this local instead.
          const metadata = this.metadata
          const publication = ++this.metadataPublications
          const isLatest = () =>
            applicable() &&
            this.metadataPublications === publication &&
            !fence?.closed &&
            !guard?.()
          // Applied here, on the statement after the assignment and before any await, so no
          // reader can observe the fresh response with the monotonic leader guard not yet
          // applied. An empty topic list is encoded as the null array, which asks for every topic
          // in the cluster - the cache prunes what such a response does not carry.
          this.metadataCache.update(metadata, {
            full: !accept && (!topics || topics.length === 0),
            isCurrent: applicable,
          })
          if (!isLatest()) return
          onMetadata?.(metadata)
          if (!isLatest()) return
          this.metadataExpireAt = Date.now() + this.metadataMaxAge

          this.setupMetadataEndpoints = new Map(
            metadata.brokers.map(({ nodeId, host, port }) => [String(nodeId), `${host}:${port}`])
          )
          this.retainSetupEndpoints()
          const previousBrokers = { ...this.brokers }
          const freshBrokerIds = new Set(metadata.brokers.map(({ nodeId }) => String(nodeId)))
          const seedAddressAdvertised = metadata.brokers.some(
            ({ host, port }) => host === seedHost && port === seedPort
          )
          for (const { nodeId, host, port, rack } of metadata.brokers) {
            const previous = previousBrokers[nodeId]
            if (
              !isLatest() ||
              this.brokers[nodeId] !== previous ||
              this.seedBroker !== seedBroker
            ) {
              continue
            }
            if (previous && !hasBrokerMoved(previous, { host, port })) {
              previous.connectionPool.rack = rack
              continue
            }
            if (seedBroker && host === seedHost && port === seedPort) {
              seedBroker.nodeId = nodeId
              seedBroker.connectionPool.rack = rack
              this.brokers[nodeId] = seedBroker
              this.closeObsolete(previous)
              continue
            }
            const ownsNode = () =>
              isLatest() && this.brokers[nodeId] === previous && this.seedBroker === seedBroker
            const connectionPool = await this.buildConnectionPool(
              { host, port, rack },
              { fence, isCurrent: ownsNode }
            )
            if (!connectionPool) continue
            if (
              !isLatest() ||
              this.brokers[nodeId] !== previous ||
              this.seedBroker !== seedBroker
            ) {
              this.closePool(connectionPool)
              continue
            }
            if (previous && previous === seedBroker && !seedAddressAdvertised) {
              this.replacePool(previous, connectionPool)
            } else {
              this.brokers[nodeId] = this.createBroker({
                logger: this.rootLogger,
                versions: this.versions,
                connectionPool,
                nodeId,
              })
              this.closeObsolete(previous)
            }
          }
          for (const [nodeId, previous] of Object.entries(previousBrokers)) {
            if (isLatest() && !freshBrokerIds.has(nodeId) && this.brokers[nodeId] === previous) {
              if (previous === this.seedBroker) delete this.brokers[nodeId]
              else this.closeObsolete(previous)
            }
          }
        } catch (e) {
          fence?.check()
          if (guard?.()) return bail(e)
          if (!applicable()) return
          if (
            e.type === 'LEADER_NOT_AVAILABLE' ||
            ((allowAutoTopicCreation ?? broker.allowAutoTopicCreation) &&
              e.type === 'UNKNOWN_TOPIC_OR_PARTITION')
          ) {
            throw e
          }

          bail(e)
        }
      })

      if (applicable()) this.onMetadataSuccess()
    } catch (e) {
      check()
      if (guard?.()) throw e
      if (!applicable()) return
      if (!fence?.closed) await this.onMetadataFailure(e, { fence })
      throw e
    }
  }

  /**
   * Drops one producer target from the retained decoded response, preserving broker discovery.
   * @param {string} topic forgotten name
   * @returns {void}
   */
  forgetTopic(topic) {
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
   * Only refreshes metadata if the data is stale according to the `metadataMaxAge` param or does not contain information about the provided topics
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
    const shouldRefresh =
      this.metadata == null ||
      this.metadataExpireAt == null ||
      Date.now() > this.metadataExpireAt ||
      !topics.every((topic) =>
        this.metadata.topicMetadata.some((topicMetadata) => topicMetadata.topic === topic)
      )

    if (shouldRefresh) {
      const context = metadataScope ? { ...options, metadataScope } : options
      return Object.keys(context).length
        ? this.refreshMetadata(topics, context)
        : this.refreshMetadata(topics)
    }
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
   */
  async findBroker({ nodeId, fence }) {
    fence?.check()
    checked(null)
    const broker = this.brokers[nodeId]

    if (!broker) {
      throw new KafkaJSBrokerNotFound(`Broker ${nodeId} not found in the cached metadata`)
    }

    await this.connectBroker(broker, ...(fence ? [{ fence }] : []))
    fence?.check()
    checked(null)
    return broker
  }

  /**
   * @public
   * @param {(params: { nodeId: string, broker: Broker }) => Promise<T>} callback
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: once
   *   it closed, no further broker is tried
   * @returns {Promise<T>}
   * @throws {Error} the lifecycle's close error
   * @template T
   */
  async withBroker(callback, { fence } = {}) {
    const brokers = shuffle(keys(this.brokers))
    if (brokers.length === 0) {
      throw new KafkaJSBrokerNotFound('No brokers in the broker pool')
    }

    for (const nodeId of brokers) {
      const broker = await this.findBroker({ nodeId, fence })
      try {
        return await callback({ nodeId, broker })
      } catch (e) {
        fence?.check()
        checked(null)
      }
    }

    return null
  }

  /**
   * @public
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: once
   *   it closed, no broker is tried, no socket opens, and a rebootstrap it started installs no pool
   * @returns {Promise<Broker>}
   * @throws {Error} the lifecycle's close error
   */
  async findConnectedBroker({ fence } = {}) {
    fence?.check()
    checked(null)
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
    fence?.check()
    checked(null)
    if (this.shouldRebootstrapOnAllBrokersUnavailable(nodeIds, allFailuresWereAuthentication)) {
      await this.rebootstrap('all-brokers-unavailable', { fence })
      fence?.check()
      checked(null)
    }

    // Failed to connect to all known brokers, metadata might be old
    await this.connect(...(fence ? [{ fence }] : []))
    fence?.check()
    checked(null)
    return this.seedBroker
  }

  /**
   * @private
   * @param {Broker} broker
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] see `findBroker`
   * @returns {Promise<null>}
   */
  async connectBroker(broker, { fence } = {}) {
    if (broker.isConnected()) {
      return
    }

    return this.retryFor(fence)(async (bail, retryCount, retryTime) => {
      try {
        fence?.check()
        checked(null)
        await broker.connect(...guardArgs(fence))
        fence?.check()
        checked(null)
      } catch (caught) {
        fence?.check()
        checked(null)
        const e = isNativeError(caught)
          ? caught
          : new KafkaJSNonRetriableError(describeThrown(caught), { cause: caught })
        if (isConnectionFailure(e) || describeThrown(e, 'type') === 'ILLEGAL_SASL_STATE') {
          await broker.disconnect()
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
          const isCurrent = () => broker.connectionPool === previousPool
          const replacement = await this.buildConnectionPool(
            { host: previousPool.host, port: previousPool.port, rack: previousPool.rack },
            { fence, isCurrent }
          )
          if (!replacement)
            return bail(new KafkaJSConnectionError('Broker pool build was superseded'))
          if (fence?.closed || currentDeadline()?.guard()) {
            this.closePool(replacement)
            fence?.check()
            checked(null)
          }
          if (!isCurrent()) {
            this.closePool(replacement)
            return bail(new KafkaJSConnectionError('Broker pool build was superseded'))
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
   * A throw is logged and dropped instead.
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
      this.instrumentationEmitter.emit(eventName, payload)
    } catch (e) {
      this.logger.error('Instrumentation event listener threw, ignoring it', {
        eventName,
        error: e.message,
      })
    }
  }
}
