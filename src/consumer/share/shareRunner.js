const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const createRetry = require('../../retry')
const { ConsumerOperation } = require('../groupProtocol/interface')
const shareCapability = require('./shareCapability')
const { ShareRun } = require('./shareRun')
const { ShareHandlerCall } = require('./shareHandlerCall')
const { ShareCloseEpisode } = require('./shareCloseEpisode')
const { events, wrap, unwrap } = require('./instrumentationEvents')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
  KafkaJSSASLAuthenticationError,
  KafkaJSShareAcknowledgementError,
  KafkaJSShareGroupsNotSupportedError,
} = require('../../errors')

const MAX_INT = 2147483647
const REQUIRED = [apiKeys.ShareGroupHeartbeat, apiKeys.ShareFetch, apiKeys.ShareAcknowledge]
const IMPLEMENTED = {
  [apiKeys.ShareGroupHeartbeat]: requests.ShareGroupHeartbeat,
  [apiKeys.ShareFetch]: requests.ShareFetch,
  [apiKeys.ShareAcknowledge]: requests.ShareAcknowledge,
}
const SUBSCRIBE_REJECTED = Object.freeze({
  topic: 'subscribe takes a topics array',
  regex: 'a share group has no regex subscription',
  fromBeginning: 'use the group config share.auto.offset.reset',
  offsetReset: 'use the group config share.auto.offset.reset',
})
/** Protocol codes that end a run for good: authorization, version, request and assignor refusals, group gone. */
const NON_RESTARTABLE_CODES = new Set([29, 30, 31, 35, 42, 69, 81])

/**
 * @param {Error} error A terminal failure.
 * @returns {Error} The deepest `cause` of the chain.
 */
const originalCause = (error) => (error.cause ? originalCause(error.cause) : error)

/**
 * Reads a `disconnect()` argument: an options object with `timeout`, or nothing. Anything that is
 * not an object is ignored, so `process.on('SIGTERM', share.disconnect)` is safe.
 * @param {*} [options] The argument.
 * @returns {number|undefined} The validated timeout in milliseconds, or undefined for a natural wait.
 * @throws {KafkaJSInvalidConfigurationError} When `timeout` is not an integer from 0 to 2147483647.
 */
const parseDisconnectTimeout = (options) => {
  if (options == null || typeof options !== 'object') return undefined
  const { timeout } = options
  if (timeout !== undefined && (!Number.isInteger(timeout) || timeout < 0 || timeout > MAX_INT)) {
    throw new KafkaJSInvalidConfigurationError(
      'Argument "timeout" must be an integer from 0 to 2147483647'
    )
  }
  return timeout
}

/**
 * Owns one public share client: connection state, the subscription, event publication and the
 * lifecycle entry points. Each `run()` creates a {@link ShareRun} with fresh membership, poll and
 * heartbeat state; this class outlives runs and decides what a run's terminal error means.
 */
class ShareRunner {
  /**
   * @param {object} options Validated factory dependencies.
   * @param {object} options.logger Namespaced logger.
   * @param {import('../../instrumentation/emitter')} options.instrumentationEmitter This client's emitter.
   * @param {object} [options.cluster] This client's own Cluster.
   * @param {number} [options.requestTimeout] Finite root request timeout, the cap of finals and leave.
   * @param {object} [options.retry] Merged retry options, including `restartOnFailure`.
   * @returns {ShareRunner} An idle owner; construction creates no sockets or timers.
   */
  constructor(options) {
    this.options = options
    this.current = null
    this.stopGeneration = 0
    this.subscription = []
    this.latestLockDuration = null
    this.connected = false
    this.connecting = null
    this.subscribing = false
    this.disconnecting = null
    this.restartOperation = null
    this.lifecycle = 0
  }

  /**
   * @returns {import('../../../types').ShareConsumer} The public facade: bound methods that never
   *   use a caller-supplied `this`, a frozen event table and the namespaced logger. The runner,
   *   its nodes and any seek, commit, pause or manual heartbeat are not exposed.
   */
  api() {
    return Object.freeze({
      connect: () => this.connect(),
      disconnect: (options) => this.disconnect(options),
      subscribe: (subscription) => this.subscribe(subscription),
      subscription: () => this.subscription.slice(),
      assignment: () => this.current?.assignmentSnapshot() ?? [],
      run: (config) => this.run(config),
      stop: () => this.stop(),
      acquisitionLockTimeoutMs: () => this.latestLockDuration,
      on: (eventName, listener) => this.on(eventName, listener),
      logger: () => this.options.logger,
      events: Object.freeze({ ...events }),
    })
  }

  /**
   * Registers a listener whose synchronous throw and rejected return are logged and contained.
   * @param {string} eventName Public name, including its `share_consumer.` prefix.
   * @param {Function} listener Application observer.
   * @returns {Function} Idempotent listener remover.
   * @throws {KafkaJSNonRetriableError} Synchronously for an unknown event name.
   */
  on(eventName, listener) {
    if (!Object.values(events).includes(eventName)) {
      throw new KafkaJSNonRetriableError(
        `Event name should be one of ${Object.keys(events).join(', ')}`
      )
    }
    const { logger } = this.options
    const report = (error) =>
      logger.error(`Failed to execute listener: ${error?.message}`, {
        eventName,
        stack: error?.stack,
      })
    return this.options.instrumentationEmitter.addListener(unwrap(eventName), (event) => {
      event.type = wrap(event.type)
      try {
        Promise.resolve(listener(event)).catch(report)
      } catch (error) {
        report(error)
      }
    })
  }

  /**
   * Publishes one event on this client's emitter and diagnostics channel.
   * @param {string} name Internal event name, one of `events`.
   * @param {object} [payload] The event payload; none for connect, disconnect and stop.
   * @returns {void}
   */
  emit(name, payload) {
    this.options.instrumentationEmitter.emit(name, payload)
  }

  /**
   * Publishes already-committed compact outcomes after ledger state has changed. Reentrant stop
   * cannot cause another settlement or expand a missing-record span.
   * @param {{topic: string, partition: number}} cohort Original topic and partition identity.
   * @param {object[]} results Compact range results, already consumed from the ledger.
   * @returns {void} Listener failures are contained by `on()`.
   */
  publishResults(cohort, results) {
    for (const result of results) {
      this.emit(events.ACKNOWLEDGE, {
        topic: cohort.topic,
        partition: cohort.partition,
        firstOffset: result.firstOffset,
        lastOffset: result.lastOffset,
        type: result.type,
        error: result.error,
      })
    }
  }

  /**
   * Refuses a broker that cannot run the required share APIs, before any share frame is sent.
   * @param {object} broker A connected Broker with its negotiated versions and finalized features.
   * @param {number[]} required The share API keys the caller is about to use.
   * @returns {void}
   * @throws {KafkaJSShareGroupsNotSupportedError} Naming the first refusal, in the order
   *   advertisement, version overlap, finalized feature.
   */
  checkCapability(broker, required) {
    const verdict = shareCapability({
      versions: broker.versions,
      finalizedFeatures: broker.finalizedFeatures,
      finalizedFeaturesEpoch: broker.finalizedFeaturesEpoch,
      implemented: IMPLEMENTED,
      required,
    })
    if (!verdict.ok)
      throw new KafkaJSShareGroupsNotSupportedError(
        `Share groups refused: ${verdict.reason}`,
        verdict
      )
  }

  /**
   * Connects the client's Cluster and checks the share capability of the bootstrap broker.
   * Concurrent and repeated calls share one attempt and one `connect` event.
   * @returns {Promise<void>}
   * @throws {KafkaJSShareGroupsNotSupportedError} When the cluster cannot run share groups; the
   *   newly opened connections are closed and no `connect` event is published.
   */
  connect() {
    if (this.connected) return Promise.resolve()
    if (!this.connecting) {
      const attempt = this.connectOnce().finally(() => {
        if (this.connecting === attempt) this.connecting = null
      })
      this.connecting = attempt
    }
    return this.connecting
  }

  /**
   * @private
   * @returns {Promise<void>}
   */
  async connectOnce() {
    const { cluster } = this.options
    const generation = this.lifecycle
    const fence = new ConsumerOperation({
      guard: () =>
        this.lifecycle === generation
          ? null
          : new KafkaJSNonRetriableError('Share client was disconnected'),
    })
    try {
      await cluster.connect({ fence })
      const broker = await cluster.findConnectedBroker({ fence })
      fence.check()
      this.checkCapability(broker, REQUIRED)
      this.connected = true
    } catch (error) {
      if (this.lifecycle === generation)
        await cluster.disconnect({ drainTimeout: 0 }).catch(() => {})
      throw error
    } finally {
      fence.finish()
    }
    this.emit(events.CONNECT)
  }

  /**
   * Replaces the subscription while no run is active. Names are validated and copied before any
   * state or request; target topics are added under an operation fence, and the subscription is
   * committed only if the client was not disconnected meanwhile.
   * @param {{topics: string[]}} input Distinct non-empty topic names.
   * @returns {Promise<void>}
   * @throws {KafkaJSInvalidConfigurationError} For any other shape, a regex or a rejected key.
   * @throws {KafkaJSNonRetriableError} While a run, a close or another subscribe is active.
   */
  async subscribe(input) {
    const names = this.parseSubscription(input)
    if (this.current || this.subscribing) {
      throw new KafkaJSNonRetriableError(
        'subscribe() cannot replace the subscription while a run, a close or another subscribe is active'
      )
    }
    this.subscribing = true
    const generation = this.lifecycle
    const fence = new ConsumerOperation({
      guard: () =>
        this.lifecycle === generation
          ? null
          : new KafkaJSNonRetriableError('Share client was disconnected'),
    })
    try {
      await this.connect()
      await this.options.cluster.addMultipleTargetTopics(names, { fence })
      fence.check()
      this.subscription = names
    } finally {
      fence.finish()
      this.subscribing = false
    }
  }

  /**
   * @private
   * @param {*} input The `subscribe` argument.
   * @returns {string[]} A fresh array of the validated names.
   * @throws {KafkaJSInvalidConfigurationError} For an invalid shape or name.
   */
  parseSubscription(input) {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
      throw new KafkaJSInvalidConfigurationError('subscribe requires an object with a topics array')
    }
    for (const [key, equivalent] of Object.entries(SUBSCRIBE_REJECTED)) {
      if (input[key] !== undefined)
        throw new KafkaJSInvalidConfigurationError(`${key} is not supported: ${equivalent}`)
    }
    const { topics } = input
    if (!Array.isArray(topics) || topics.length === 0) {
      throw new KafkaJSInvalidConfigurationError('topics must be a non-empty array of topic names')
    }
    const seen = new Set()
    for (const topic of topics) {
      if (typeof topic !== 'string' || topic.length === 0 || seen.has(topic)) {
        throw new KafkaJSInvalidConfigurationError(
          'topics must be distinct non-empty strings; regular expressions are not supported'
        )
      }
      seen.add(topic)
    }
    return [...seen]
  }

  /**
   * Starts a run with exactly one handler. The run is latched before the first await, so a stop
   * cannot miss a start in progress. Resolves after the first accepted join.
   * @param {{eachMessage?: Function, eachBatch?: Function}} config Exactly one handler.
   * @returns {Promise<void>} Settles after the first accepted join; resolves void when stop ends
   *   a retriable pending join; rejects with the original fatal join error.
   * @throws {KafkaJSInvalidConfigurationError} Without exactly one handler function.
   * @throws {KafkaJSNonRetriableError} Without a subscription, or while a run or close is active.
   */
  async run(config) {
    const eachMessage = config?.eachMessage
    const eachBatch = config?.eachBatch
    const batch = eachMessage === undefined
    const handler = batch ? eachBatch : eachMessage
    if (
      (eachMessage === undefined) === (eachBatch === undefined) ||
      typeof handler !== 'function'
    ) {
      throw new KafkaJSInvalidConfigurationError(
        'run requires exactly one of eachMessage or eachBatch'
      )
    }
    if (this.subscription.length === 0)
      throw new KafkaJSNonRetriableError('subscribe() must be called before run()')
    if (this.current || this.subscribing) {
      throw new KafkaJSNonRetriableError(
        'run() cannot start while another run, close or subscribe is active'
      )
    }
    const run = this.newRun(handler, batch)
    try {
      await this.connect()
    } catch (error) {
      run.finishRun()
      throw error
    }
    if (!run.stopRequested && this.current === run) run.start()
    return run.firstJoin.promise
  }

  /**
   * @private
   * @param {Function} handler The application handler.
   * @param {boolean} batch Whether it is `eachBatch`.
   * @returns {ShareRun} The new current run, with a null lock-duration snapshot.
   */
  newRun(handler, batch) {
    const run = new ShareRun({ runner: this, handler, batch })
    this.current = run
    this.latestLockDuration = null
    return run
  }

  /**
   * Stops the current run: running handlers finish, then captured finals and one leave go out.
   * A handler must call this without awaiting it.
   * @returns {Promise<void>} Resolves when the close ended; at once without a run.
   */
  stop() {
    const run = this.current
    if (!run) return Promise.resolve()
    this.stopGeneration++
    this.restartOperation?.retire(new KafkaJSNonRetriableError('Share client was stopped'))
    return run.stop()
  }

  /**
   * Closes the run, if any, and then this client's sockets. The first valid call fixes the whole
   * deadline; later calls share its close and cannot extend it, and timeout zero revokes unsent
   * closing traffic synchronously.
   * @param {{timeout?: number}|*} [options] Options object; any other argument is ignored.
   * @returns {Promise<void>}
   * @throws {KafkaJSInvalidConfigurationError} For an invalid timeout, before any state changes.
   */
  async disconnect(options) {
    const enteredAt = performance.now()
    const timeout = parseDisconnectTimeout(options)
    const run = this.current
    if (this.disconnecting) {
      run?.beginClose('disconnect', timeout, enteredAt)
      return this.disconnecting
    }
    this.lifecycle++
    this.stopGeneration++
    this.restartOperation?.retire(new KafkaJSNonRetriableError('Share client was disconnected'))
    const episode = run?.beginClose('disconnect', timeout, enteredAt)
    this.disconnecting = this.closeClient(episode)
    return this.disconnecting
  }

  /**
   * @private
   * @param {import('./shareCloseEpisode').ShareCloseEpisode} [episode] The run's close, if any.
   * @returns {Promise<void>}
   */
  async closeClient(episode) {
    try {
      await episode?.done
      await this.options.cluster.disconnect({ drainTimeout: 0 })
      const wasConnected = this.connected
      this.connected = false
      if (wasConnected) this.emit(events.DISCONNECT)
    } finally {
      this.disconnecting = null
    }
  }

  /**
   * Decides what a terminal run error means: the restart verdict, the crash event, and either
   * the end of the run or a fresh run after a cancellable retry delay. The client's stop
   * generation and ownership are read again after the cleanup, the callback and the delay.
   * @param {ShareRun} run The crashed run, already cleaned up.
   * @param {Error} error The exact terminal error.
   * @returns {Promise<void>}
   */
  async superviseCrash(run, error) {
    const { logger, retry = {}, groupId } = this.options
    logger.error(`Crash: ${error.name}: ${error.message}`, {
      groupId,
      retryCount: error.retryCount,
      stack: error.stack,
    })
    let restart =
      error.name === 'KafkaJSNumberOfRetriesExceeded' || originalCause(error).retriable === true
    if (retry.restartOnFailure) {
      try {
        restart = await retry.restartOnFailure(error)
      } catch (callbackError) {
        logger.error(
          'Caught error when invoking user-provided "restartOnFailure" callback. Defaulting to restarting.',
          { error: callbackError?.message || callbackError, cause: error.message, groupId }
        )
        restart = true
      }
    }
    if (this.mustNotRestart(error)) restart = false
    const owned =
      this.current === run && run.stopGeneration === this.stopGeneration && !run.stopRequested
    if (restart && !owned) {
      logger.warn('Share consumer was stopped while deciding whether to restart; not restarting', {
        groupId,
      })
      restart = false
    }
    this.emit(events.CRASH, { error, groupId, restart })
    if (!restart) {
      run.finishRun()
      return
    }
    const retryTime =
      error.retryTime || retry.initialRetryTime || createRetry.defaults.initialRetryTime
    logger.error(`Restarting the share consumer in ${retryTime}ms`, { groupId })
    const operation = new ConsumerOperation()
    this.restartOperation = operation
    try {
      await operation.sleep(retryTime)
    } catch {
      run.finishRun(false)
      if (this.current === run) this.current = null
      return
    } finally {
      operation.finish()
      if (this.restartOperation === operation) this.restartOperation = null
    }
    run.finishRun(false)
    if (this.current !== run || run.stopGeneration !== this.stopGeneration) {
      if (this.current === run) this.current = null
      return
    }
    this.newRun(run.handler, run.batch).start()
  }

  /**
   * @private
   * @param {Error} error A terminal failure.
   * @returns {boolean} True for configuration, authentication, broker-refusal and omission
   *   failures and the fatal membership codes, which override `restartOnFailure`.
   */
  mustNotRestart(error) {
    const cause = originalCause(error)
    return (
      error instanceof KafkaJSInvalidConfigurationError ||
      cause instanceof KafkaJSInvalidConfigurationError ||
      cause instanceof KafkaJSShareGroupsNotSupportedError ||
      cause instanceof KafkaJSSASLAuthenticationError ||
      (error instanceof KafkaJSShareAcknowledgementError && error.reason === 'unacknowledged') ||
      NON_RESTARTABLE_CODES.has(cause.code)
    )
  }
}

module.exports = { ShareRunner, ShareHandlerCall, ShareCloseEpisode }
