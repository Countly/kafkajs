const Broker = require('../broker')
const MetadataCache = require('./metadataCache')
const createRetry = require('../retry')
const shuffle = require('../utils/shuffle')
const arrayDiff = require('../utils/arrayDiff')
const sharedPromiseTo = require('../utils/sharedPromiseTo')
const { CLUSTER_REBOOTSTRAP } = require('./instrumentationEvents')
const { KafkaJSBrokerNotFound, KafkaJSProtocolError } = require('../errors')

const { keys, assign, values } = Object
const hasBrokerBeenReplaced = (broker, { host, port, rack }) =>
  broker.connectionPool.host !== host ||
  broker.connectionPool.port !== port ||
  broker.connectionPool.rack !== rack

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
   * @param {import("./metadataCache")} [options.metadataCache] defaulted rather than required:
   *   `BrokerPool` is also constructed directly by its own integration spec, and a pool without a
   *   `Cluster` around it still needs somewhere to put topic ids and leader epochs. `Cluster`
   *   always injects its own, which is the instance every route reads.
   * @param {import("../instrumentation/emitter")|null} [options.instrumentationEmitter]
   * @param {import("../../types").RetryOptions} [options.retry]
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
  }) {
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
      const brokers = values(this.brokers)
      const seedBroker = this.seedBroker

      this.brokers = {}
      this.metadata = null
      this.metadataExpireAt = null
      this.metadataCache.clear()

      await Promise.all([
        ...brokers.map((broker) => broker.disconnect()),
        seedBroker ? seedBroker.disconnect() : Promise.resolve(),
      ])

      this.seedBroker = undefined
      await this.createSeedBroker()

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
   * `this.brokers` is emptied *before* the disconnects are awaited, so a concurrent `findBroker`
   * or `findConnectedBroker` cannot be handed a broker that is mid-teardown. Each disconnect
   * itself first *drains* that broker's pending requests (`Connection#disconnect` awaits
   * `requestQueue.waitForPendingRequests()`) before closing the socket, so an in-flight request
   * normally completes rather than being rejected - the wait is bounded only by
   * `enforceRequestTimeout`'s interval; with it disabled, a half-open socket can stall this
   * teardown, and every later caller (`sharedPromiseTo`-joined) stalls with it.
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

  async createSeedBroker() {
    if (this.seedBroker) {
      await this.seedBroker.disconnect()
    }

    const connectionPool = await this.buildConnectionPool()

    this.seedBroker = this.createBroker({
      connectionPool,
      logger: this.rootLogger,
    })
  }

  /**
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
      try {
        await this.seedBroker.connect()
        this.versions = this.seedBroker.versions
      } catch (e) {
        if (e.name === 'KafkaJSConnectionError' || e.type === 'ILLEGAL_SASL_STATE') {
          // Connection builder will always rotate the seed broker
          await this.createSeedBroker()
          this.logger.error(
            `Failed to connect to seed broker, trying another broker from the list: ${e.message}`,
            { retryCount, retryTime }
          )
        } else {
          this.logger.error(e.message, { retryCount, retryTime })
        }

        if (e.retriable) throw e
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
   * @public
   * @param {Object} destination
   * @param {string} destination.host
   * @param {number} destination.port
   */
  removeBroker({ host, port }) {
    const removedBroker = values(this.brokers).find(
      (broker) => broker.connectionPool.host === host && broker.connectionPool.port === port
    )

    if (removedBroker) {
      delete this.brokers[removedBroker.nodeId]
      this.metadataExpireAt = null

      if (this.seedBroker.nodeId === removedBroker.nodeId) {
        this.seedBroker = shuffle(values(this.brokers))[0]
      }
    }
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
        // both of `Cluster`'s shared-promise guards) sets `this.seedBroker = undefined` partway
        // through its own teardown, and re-reading `this.seedBroker` after the `await` below
        // would then throw a `TypeError` - which the retrier classifies as unrecoverable and
        // never retries.
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
          // applied.
          this.metadataCache.update(metadata)
          this.metadataExpireAt = Date.now() + this.metadataMaxAge

          const replacedBrokers = []

          // `this.brokers` here is read once, synchronously, as the reduce's starting value - a
          // rebootstrap that empties it while this reduce is still running is overwritten by the
          // reduce's own result below. Harmless rather than wrong: `onMetadataSuccess()` still
          // declines to arm, because by then `this.metadata` is back to whatever the rebootstrap
          // left it as, not this response.
          this.brokers = await metadata.brokers.reduce(
            async (resultPromise, { nodeId, host, port, rack }) => {
              const result = await resultPromise

              if (result[nodeId]) {
                if (!hasBrokerBeenReplaced(result[nodeId], { host, port, rack })) {
                  return result
                }

                replacedBrokers.push(result[nodeId])
              }

              if (seedBroker && host === seedHost && port === seedPort) {
                seedBroker.nodeId = nodeId
                seedBroker.connectionPool.rack = rack
                return assign(result, {
                  [nodeId]: seedBroker,
                })
              }

              return assign(result, {
                [nodeId]: this.createBroker({
                  logger: this.rootLogger,
                  versions: this.versions,
                  connectionPool: await this.buildConnectionPool({ host, port, rack }),
                  nodeId,
                }),
              })
            },
            this.brokers
          )

          const freshBrokerIds = metadata.brokers.map(({ nodeId }) => `${nodeId}`).sort()
          const currentBrokerIds = keys(this.brokers).sort()
          const unusedBrokerIds = arrayDiff(currentBrokerIds, freshBrokerIds)

          const brokerDisconnects = unusedBrokerIds.map((nodeId) => {
            const broker = this.brokers[nodeId]
            return broker.disconnect().then(() => {
              delete this.brokers[nodeId]
            })
          })

          const replacedBrokersDisconnects = replacedBrokers.map((broker) => broker.disconnect())
          await Promise.all([...brokerDisconnects, ...replacedBrokersDisconnects])
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
      } catch (e) {
        if (e.name === 'KafkaJSConnectionError' || e.type === 'ILLEGAL_SASL_STATE') {
          await broker.disconnect()
        }

        // To avoid reconnecting to an unavailable host, we bail on connection errors
        // and refresh metadata on a higher level before reconnecting
        if (e.name === 'KafkaJSConnectionError') {
          return bail(e)
        }

        if (e.type === 'ILLEGAL_SASL_STATE') {
          // Rebuild the connection pool since it can't recover from illegal SASL state.
          // replaceConnectionPool also resets versionsGeneration, so the next connect() always
          // re-negotiates against the replacement pool (KIP-35 rule 7) rather than comparing
          // against the generation the old, discarded pool had reached.
          broker.replaceConnectionPool(
            await this.buildConnectionPool({
              host: broker.connectionPool.host,
              port: broker.connectionPool.port,
              rack: broker.connectionPool.rack,
            })
          )

          this.logger.error(`Failed to connect to broker, reconnecting`, { retryCount, retryTime })
          throw new KafkaJSProtocolError(e, { retriable: true })
        }

        if (e.retriable) throw e
        this.logger.error(e, { retryCount, retryTime, stack: e.stack })
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
   * Emits a cluster-level instrumentation event, mirroring `MetadataCache#emit`.
   * @private
   * @param {string} eventName
   * @param {object} payload
   */
  emit(eventName, payload) {
    this.instrumentationEmitter && this.instrumentationEmitter.emit(eventName, payload)
  }
}
