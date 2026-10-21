const { isNativeError } = require('util').types
const describeThrown = require('../network/describeThrown')
const Broker = require('../broker')
const MetadataCache = require('./metadataCache')
const createRetry = require('../retry')
const shuffle = require('../utils/shuffle')
const sharedPromiseTo = require('../utils/sharedPromiseTo')
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
   * @param {object} options
   * @param {import("./connectionPoolBuilder").ConnectionPoolBuilder} options.connectionPoolBuilder
   * @param {import("../../types").Logger} options.logger
   * @param {import("./metadataCache")} [options.metadataCache] optional: a `BrokerPool` owns its
   *   metadata and is usable standalone, so it defaults to a private cache rather than requiring
   *   a collaborator it can build itself. `Cluster` always injects its own, which is then the
   *   single instance every route reads.
   * @param {import("../instrumentation/emitter")|null} [options.instrumentationEmitter]
   * @param {import("../../types").RetryOptions} [options.retry]
   * @param {number} [options.drainTimeout] obsolete-resource drain bound in milliseconds
   * @param {boolean} [options.allowAutoTopicCreation]
   * @param {number} [options.authenticationTimeout]
   * @param {number} [options.metadataMaxAge]
   * @param {'rebootstrap'|'none'} [options.metadataRecoveryStrategy='rebootstrap'] KIP-899/1102:
   *   `'rebootstrap'` enables all three triggers in `refreshMetadata`/`findConnectedBroker`;
   *   `'none'` disables them and leaves `REBOOTSTRAP_REQUIRED` (129) as a plain protocol error.
   * @param {number} [options.metadataRecoveryRebootstrapTriggerMs=300000] how long metadata must
   *   fail *continuously* (trigger (a)) before a rebootstrap fires.
   */
  constructor({
    connectionPoolBuilder,
    logger,
    metadataCache,
    instrumentationEmitter,
    retry,
    allowAutoTopicCreation,
    authenticationTimeout,
    metadataMaxAge,
    metadataRecoveryStrategy,
    metadataRecoveryRebootstrapTriggerMs,
    drainTimeout,
  }) {
    this.drainTimeout = drainTimeout
    this.retiredBrokers = new WeakSet()
    this.rootLogger = logger
    this.connectionPoolBuilder = connectionPoolBuilder
    this.metadataMaxAge = metadataMaxAge || 0
    this.logger = logger.namespace('BrokerPool')
    this.retrier = createRetry(assign({}, retry))
    this.authenticationTimeout = authenticationTimeout ?? 10000
    this.metadataCache = metadataCache ?? new MetadataCache({ logger })
    this.instrumentationEmitter = instrumentationEmitter ?? null

    // Broker address -> blackout entry, shared by every ConnectionPool this BrokerPool builds
    // (see buildConnectionPool), so a pool rebuilt after a connection error (seed rotation,
    // illegal SASL state) keeps the blackout instead of starting over with an empty one.
    this.authenticationBlackout = new Map()

    this.createBroker = (options) =>
      new Broker({
        allowAutoTopicCreation,
        authenticationTimeout,
        ...options,
      })

    this.brokers = {}
    /** @type {Broker | undefined} */
    this.seedBroker = undefined
    /** @type {import("../../types").BrokerMetadata | null} */
    this.metadata = null
    this.metadataExpireAt = null
    this.versions = null

    this.metadataRecoveryStrategy = metadataRecoveryStrategy ?? 'rebootstrap'
    this.metadataRecoveryRebootstrapTriggerMs = metadataRecoveryRebootstrapTriggerMs ?? 300000

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

    this[PRIVATE.REBOOTSTRAP] = sharedPromiseTo(async (reason) => {
      const brokers = new Set([this.seedBroker, ...values(this.brokers)])
      this.metadata = null
      this.metadataExpireAt = null
      this.metadataCache.clear()

      // The replacement seed is built, installed and only then is the previous one disconnected
      // (see createSeedBroker), so `this.seedBroker` always points at a usable broker: a
      // concurrent reader - `connect()`'s retry callback, `hasConnectedBrokers()` - landing in
      // this window must never find it unset.
      try {
        await this.createSeedBroker()
      } finally {
        for (const broker of brokers) this.closeObsolete(broker)
      }

      this.lastRebootstrapAt = Date.now()
      // Both clocks reset, and they are reset for different reasons. `metadataUnavailableSince`
      // is trigger (a)'s continuous-failure clock: clearing it means the next failure starts a
      // fresh window, so a client that keeps failing rebootstraps once per window, every window.
      // `rebootstrapArmed` is trigger (c)'s one-shot: only a *successful* metadata response
      // re-arms it, so an unreachable cluster cannot produce one teardown per retry.
      this.metadataUnavailableSince = null
      this.rebootstrapArmed = false

      this.logger.warn('Rebootstrapping the client from the configured broker list', { reason })
      this.emit(CLUSTER_REBOOTSTRAP, { reason })
    })
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
   * Concurrent callers join one in-flight rebootstrap.
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
   * @returns {Promise<void>}
   */
  async rebootstrap(reason) {
    return this[PRIVATE.REBOOTSTRAP](reason)
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
   * @returns {Promise<import("../network/connectionPool")>}
   */
  async buildConnectionPool(destination) {
    const connectionPool = await this.connectionPoolBuilder.build(destination)
    connectionPool.authenticationTimeout = this.authenticationTimeout
    connectionPool.authenticationBlackout = this.authenticationBlackout
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
   * @returns {Promise<void>}
   */
  async applyNodeEndpoints(nodeEndpoints) {
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
      const connectionPool = await this.buildConnectionPool({ host, port, rack })

      if (this.brokers[nodeId] !== broker || (isSeed && this.seedBroker !== broker)) {
        this.closePool(connectionPool)
        continue
      }

      if (isSeed) {
        this.replacePool(broker, connectionPool)
        continue
      }

      this.brokers[nodeId] = this.createBroker({
        logger: this.rootLogger,
        versions: this.versions,
        connectionPool,
        nodeId,
      })
      this.closeObsolete(broker)
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
   * @public
   * @returns {Promise<void>}
   */
  async createSeedBroker() {
    const connectionPool = await this.buildConnectionPool()
    const previousSeedBroker = this.seedBroker

    this.seedBroker = this.createBroker({
      connectionPool,
      logger: this.rootLogger,
    })

    this.closeObsolete(previousSeedBroker)
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
   * @returns {Promise<void>}
   */
  async connect() {
    if (this.hasConnectedBrokers()) {
      return
    }

    if (!this.seedBroker) {
      await this.createSeedBroker()
    }

    return this.retrier(async (bail, retryCount, retryTime) => {
      const seedBroker = this.seedBroker

      if (!seedBroker) {
        throw new KafkaJSConnectionError('No seed broker available, the client is rebootstrapping')
      }

      try {
        await seedBroker.connect()
        this.versions = seedBroker.versions
      } catch (caught) {
        const e = isNativeError(caught)
          ? caught
          : new KafkaJSNonRetriableError(describeThrown(caught), { cause: caught })
        if (isConnectionFailure(e) || describeThrown(e, 'type') === 'ILLEGAL_SASL_STATE') {
          // Connection builder will always rotate the seed broker
          await this.createSeedBroker()
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
   * @public
   * @returns {Promise}
   */
  async disconnect() {
    this.seedBroker && (await this.seedBroker.disconnect())
    await Promise.all(values(this.brokers).map((broker) => broker.disconnect()))

    this.brokers = {}
    this.metadata = null
    this.versions = null
    this.authenticationBlackout.clear()
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
   * @returns {Promise<void>}
   */
  async refreshMetadata(topics) {
    try {
      await this.retrier(async (bail, retryCount, retryTime) => {
        let broker
        try {
          broker = await this.findConnectedBroker()
        } catch (err) {
          return bail(err)
        }

        // Captured once, before any `await` in this callback: a concurrent rebootstrap (reachable
        // because `Cluster#metadata()` calls `refreshMetadataIfNecessary` directly, bypassing
        // both of `Cluster`'s shared-promise guards) swaps `this.seedBroker` for a fresh instance
        // partway through, and re-reading it after the `await` below would mix the two seeds'
        // state within one attempt.
        const seedBroker = this.seedBroker
        const { host: seedHost, port: seedPort } = seedBroker ? seedBroker.connectionPool : {}

        try {
          this.metadata = await broker.metadata(topics)
          // Captured synchronously, in the same statement-tick as the assignment above and
          // before anything below can `await` - a concurrent rebootstrap nulls `this.metadata`
          // as soon as it starts (see the `seedBroker` capture above for why this concurrency is
          // reachable), and the reduce below awaits once per broker. Reading `this.metadata`
          // again after those awaits - as this used to - risks a `TypeError` the retrier treats
          // as unrecoverable, which would both escape to the caller undiagnosed and (wrongly)
          // start trigger (a)'s clock over an internal race rather than real broker
          // unavailability. Everything below reads this local instead.
          const metadata = this.metadata
          // Applied here, on the statement after the assignment and before any await, so no
          // reader can observe the fresh response with the monotonic leader guard not yet
          // applied. An empty topic list is encoded as the null array, which asks for every topic
          // in the cluster - the cache prunes what such a response does not carry.
          this.metadataCache.update(metadata, { full: !topics || topics.length === 0 })
          this.metadataExpireAt = Date.now() + this.metadataMaxAge

          const previousBrokers = { ...this.brokers }
          const freshBrokerIds = new Set(metadata.brokers.map(({ nodeId }) => String(nodeId)))
          const seedAddressAdvertised = metadata.brokers.some(
            ({ host, port }) => host === seedHost && port === seedPort
          )
          for (const { nodeId, host, port, rack } of metadata.brokers) {
            const previous = previousBrokers[nodeId]
            if (this.brokers[nodeId] !== previous || this.seedBroker !== seedBroker) continue
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
            const connectionPool = await this.buildConnectionPool({ host, port, rack })
            if (this.brokers[nodeId] !== previous || this.seedBroker !== seedBroker) {
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
            if (!freshBrokerIds.has(nodeId) && this.brokers[nodeId] === previous) {
              if (previous === this.seedBroker) delete this.brokers[nodeId]
              else this.closeObsolete(previous)
            }
          }
        } catch (e) {
          if (
            e.type === 'LEADER_NOT_AVAILABLE' ||
            (broker.allowAutoTopicCreation && e.type === 'UNKNOWN_TOPIC_OR_PARTITION')
          ) {
            throw e
          }

          bail(e)
        }
      })

      this.onMetadataSuccess()
    } catch (e) {
      await this.onMetadataFailure(e)
      throw e
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
   * @returns {Promise<void>}
   */
  async onMetadataFailure(e) {
    if (this.metadataRecoveryStrategy !== 'rebootstrap') {
      return
    }

    // Trigger (b): the broker told us outright. Deliberately not rate-limited, and it shares
    // rebootstrap()'s promise with any concurrent caller. This is unbounded and it does amplify:
    // `rebootstrap()` nulls `this.metadata`, so `refreshMetadataIfNecessary` always refreshes
    // again next call - a broker that persistently answers 129 produces one full teardown and one
    // REBOOTSTRAP event per caller iteration (e.g. per consumer fetch), throttled only by the
    // caller's own retry/backoff.
    if (e.type === 'REBOOTSTRAP_REQUIRED') {
      await this.rebootstrap('required')
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
      await this.rebootstrap('timeout')
    }
  }

  /**
   * Only refreshes metadata if the data is stale according to the `metadataMaxAge` param or does not contain information about the provided topics
   *
   * @public
   * @param {Array<String>} topics
   * @returns {Promise<null>}
   */
  async refreshMetadataIfNecessary(topics) {
    const shouldRefresh =
      this.metadata == null ||
      this.metadataExpireAt == null ||
      Date.now() > this.metadataExpireAt ||
      !topics.every((topic) =>
        this.metadata.topicMetadata.some((topicMetadata) => topicMetadata.topic === topic)
      )

    if (shouldRefresh) {
      return this.refreshMetadata(topics)
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
   * @returns {Promise<Broker>}
   */
  async findBroker({ nodeId }) {
    const broker = this.brokers[nodeId]

    if (!broker) {
      throw new KafkaJSBrokerNotFound(`Broker ${nodeId} not found in the cached metadata`)
    }

    await this.connectBroker(broker)
    return broker
  }

  /**
   * @public
   * @param {(params: { nodeId: string, broker: Broker }) => Promise<T>} callback
   * @returns {Promise<T>}
   * @template T
   */
  async withBroker(callback) {
    const brokers = shuffle(keys(this.brokers))
    if (brokers.length === 0) {
      throw new KafkaJSBrokerNotFound('No brokers in the broker pool')
    }

    for (const nodeId of brokers) {
      const broker = await this.findBroker({ nodeId })
      try {
        return await callback({ nodeId, broker })
      } catch (e) {}
    }

    return null
  }

  /**
   * @public
   * @returns {Promise<Broker>}
   */
  async findConnectedBroker() {
    const nodeIds = shuffle(keys(this.brokers))
    const connectedBrokerId = nodeIds.find((nodeId) => this.brokers[nodeId].isConnected())

    if (connectedBrokerId) {
      return await this.findBroker({ nodeId: connectedBrokerId })
    }

    // Cycle through the nodes until one connects. Tracked alongside: whether every failure seen
    // was a SASL authentication failure - trigger (c) below must never fire off the back of
    // blacked-out credentials alone (the cluster is reachable; only this client's credentials are
    // bad), so a single non-auth failure is what allows it to fire at all.
    let allFailuresWereAuthentication = nodeIds.length > 0
    for (const nodeId of nodeIds) {
      try {
        return await this.findBroker({ nodeId })
      } catch (e) {
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
    if (this.shouldRebootstrapOnAllBrokersUnavailable(nodeIds, allFailuresWereAuthentication)) {
      await this.rebootstrap('all-brokers-unavailable')
    }

    // Failed to connect to all known brokers, metadata might be old
    await this.connect()
    return this.seedBroker
  }

  /**
   * @private
   * @param {Broker} broker
   * @returns {Promise<null>}
   */
  async connectBroker(broker) {
    if (broker.isConnected()) {
      return
    }

    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        await broker.connect()
      } catch (caught) {
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
          this.replacePool(
            broker,
            await this.buildConnectionPool({
              host: broker.connectionPool.host,
              port: broker.connectionPool.port,
              rack: broker.connectionPool.rack,
            })
          )

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
          this.logger.error(e, { retryCount, retryTime, stack: describeThrown(e, 'stack') })
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
