const { EventEmitter } = require('events')
const { performance } = require('node:perf_hooks')
const Long = require('../utils/long')
const createRetry = require('../retry')
const sleep = require('../utils/sleep')
const describeThrown = require('../network/describeThrown')
const {
  isKafkaJSError,
  isFencedInstance,
  isUnknownMemberId,
  KafkaJSCommitFailedError,
  KafkaJSNonRetriableError,
  KafkaJSRebalanceListenerError,
  COORDINATOR_LOAD_IN_PROGRESS_CODE,
} = require('../errors')
const handlerContext = require('./handlerContext')
const RebalanceHooks = require('./rebalanceHooks')

const {
  events: { FETCH, FETCH_START, START_BATCH_PROCESS, END_BATCH_PROCESS },
} = require('./instrumentationEvents')
const createFetchManager = require('./fetchManager')
const RebalanceCoordinator = require('./rebalanceCoordinator')
const PollTimer = require('./pollTimer')
const Outage = require('./outage')
const { FetchOutage } = Outage
const HeartbeatManager = require('./heartbeatManager')
const {
  REBALANCE_PATHS,
  isOffsetCommitError,
  rebalanceReason,
  isConsumerProtocolFatal,
  sentOwnership,
  sentMembership,
  obsoleteOwnership,
  consumerStoppingError,
} = require('./rebalanceErrorContract')
const { COOPERATIVE, ConsumerOperation, isCallDeadline } = require('./groupProtocol/interface')

/** A coordinator that is not, or no longer, the group's (Kafka error code 16). */
const NOT_COORDINATOR = 16

const isSameOffset = (offsetA, offsetB) => Long.fromValue(offsetA).equals(Long.fromValue(offsetB))
const CONSUMING_START = 'consuming-start'
const CONSUMING_STOP = 'consuming-stop'
const HANDLERS_IDLE = 'handlers-idle'

module.exports = class Runner extends EventEmitter {
  /**
   * @param {object} options
   * @param {import("../../types").Logger} options.logger
   * @param {import("./consumerGroup")} options.consumerGroup
   * @param {import("../instrumentation/emitter")} options.instrumentationEmitter
   * @param {boolean} [options.eachBatchAutoResolve=true]
   * @param {number} options.concurrency
   * @param {(payload: import("../../types").EachBatchPayload) => Promise<void>} [options.eachBatch]
   * @param {(payload: import("../../types").EachMessagePayload) => Promise<void>} [options.eachMessage]
   * @param {number} [options.heartbeatInterval]
   * @param {number} options.maxPollInterval the resolved poll deadline, in milliseconds
   * @param {number} [options.sessionTimeout=30000] how long a classic member's fetch loop rides
   *   out lost connections and coordinator moves before they count against `retry.retries`
   * @param {(reason: Error) => void} options.onCrash
   * @param {() => void} [options.onJoinComplete] forwarded to the rebalance coordinator
   * @param {import("../../types").RetryOptions} [options.retry]
   * @param {boolean} [options.autoCommit=true]
   * @param {0|1} [options.maxPrefetchBatches=0] reserved for a fetch layer that runs one Fetch
   *   ahead of the workers. Accepted and stored; inert in this release, and read by nothing.
   */
  constructor({
    logger,
    consumerGroup,
    instrumentationEmitter,
    eachBatchAutoResolve = true,
    concurrency,
    eachBatch,
    eachMessage,
    heartbeatInterval,
    maxPollInterval,
    sessionTimeout = 30000,
    onCrash,
    onJoinComplete,
    retry,
    autoCommit = true,
    maxPrefetchBatches = 0,
  }) {
    super()
    this.logger = logger.namespace('Runner')
    this.consumerGroup = consumerGroup
    this.instrumentationEmitter = instrumentationEmitter
    this.eachBatchAutoResolve = eachBatchAutoResolve
    this.eachBatch = eachBatch
    this.eachMessage = eachMessage
    this.heartbeatInterval = heartbeatInterval
    // The fetch loop is one retrier call for as long as the consumer runs, so a total-time
    // deadline would crash it on the first failure after that time. Every other retried
    // operation of the runner keeps the deadline.
    this.fetchRetrier = createRetry(Object.assign({}, retry, { maxTotalTime: undefined }))
    this.commitRetrier = createRetry(Object.assign({}, retry))
    this.retryOptions = Object.assign({}, retry)
    this.backoff = createRetry.backoff(retry)
    /**
     * The transport outages a group member's fetch loop rides out, one per class of the client's own
     * requests in it: each ends only when a request of its own class succeeds.
     */
    this.outages = {
      fetch: new FetchOutage(consumerGroup.kind === 'consumer' ? Infinity : sessionTimeout),
      heartbeat: new Outage(consumerGroup.kind === 'consumer' ? Infinity : sessionTimeout),
      commit: new Outage(consumerGroup.kind === 'consumer' ? Infinity : sessionTimeout),
    }
    /** @type {WeakMap<object, 'fetch'|'heartbeat'|'handler'>} where an error left the loop's work */
    this.requestClasses = new WeakMap()
    /** @type {WeakMap<Error, number>} the broker whose Fetch raised an error, across manager retries */
    this.fetchNodes = new WeakMap()
    this.onCrash = onCrash
    this.autoCommit = autoCommit
    this.maxPrefetchBatches = maxPrefetchBatches
    this.fetchManager = createFetchManager({
      logger: this.logger,
      getNodeIds: () => this.consumerGroup.getNodeIds(),
      fetch: (nodeId) => this.fetch(nodeId),
      retryDelay: (error, nodeId) => this.fetchRetryDelay(error, nodeId),
      onRetire: (nodeId) => this.outages.fetch.end(nodeId),
      handler: (batch) => this.handleBatch(batch),
      concurrency,
    })

    /**
     * The run's awaited rebalance callbacks, or null without a listener: every listener-only branch
     * below is gated on it, so a run without one keeps its existing paths.
     * @type {import('./rebalanceHooks')|null}
     */
    this.rebalanceHooks = consumerGroup.rebalanceHooks ?? null
    /** Admission-only stop used by a listener's eager round; unlike `quiescing` it is not a loss. */
    this.admissionQuiescing = false

    this.running = false
    this.consuming = false
    this.stopRequested = false
    /** Whether the synchronous half of a stop has run; it runs once. */
    this.stopLatched = false
    /** @type {Error|null} the first terminal reason of a forced close; the run refuses all later work */
    this.retirement = null
    /** Handlers currently inside `eachMessage`/`eachBatch`. */
    this.inFlightHandlers = 0
    /** Set while an eager rebalance is stopping the world; see `handleRebalanceError`. */
    this.quiescing = false
    this.pollTimer = new PollTimer({ pollDeadline: maxPollInterval })
    this.heartbeatManager = new HeartbeatManager({
      groupMembership: consumerGroup,
      pollTimer: this.pollTimer,
      heartbeatInterval,
      logger: this.logger,
      // Closures, not references: the coordinator is built on the next line and the manager is
      // its collaborator, so the two can only be tied together at call time.
      onError: (error) => this.onHeartbeatError(error),
      onPollExpired: ({ elapsedMs }) =>
        this.rebalanceCoordinator.onPollExpired({
          elapsedMs,
          isStatic: Boolean(this.consumerGroup.groupInstanceId),
        }),
      isRebalancing: () => this.rebalanceCoordinator.isRebalancing(),
      guard: () => this.retirement ?? null,
    })
    this.rebalanceCoordinator = new RebalanceCoordinator({
      runner: this,
      consumerGroup,
      fetchManager: this.fetchManager,
      heartbeatManager: this.heartbeatManager,
      pollTimer: this.pollTimer,
      instrumentationEmitter,
      logger: this.logger,
      groupId: consumerGroup.groupId,
      onJoinComplete,
    })

    consumerGroup.attach({
      rebalanceCoordinator: this.rebalanceCoordinator,
      heartbeatManager: this.heartbeatManager,
    })
  }

  /**
   * Routes whatever a background heartbeat threw. Classification lives here rather than in the
   * manager, because it needs the error contract and the rebalance mode.
   * @param {Error} error
   * @returns {void}
   */
  onHeartbeatError(error) {
    const verdict = this.rebalanceCoordinator.classify(error, REBALANCE_PATHS.HEARTBEAT)

    if (!verdict) {
      // Nothing else reports this one: the timer discards the tick's promise, so an authorization
      // failure or a connection error on the heartbeat path would otherwise be silent until the
      // fetch loop happens to hit the same error.
      this.logger.debug('Background heartbeat failed without a rebalance verdict', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        path: REBALANCE_PATHS.HEARTBEAT,
        membershipProtocol: this.consumerGroup.kind || 'classic',
        error: error.message || error,
        type: error.type,
        name: error.name,
      })

      return
    }

    this.rebalanceCoordinator.requestRejoin(verdict)
  }

  get consuming() {
    return this._consuming
  }

  set consuming(value) {
    if (this._consuming !== value) {
      this._consuming = value
      this.emit(value ? CONSUMING_START : CONSUMING_STOP)
    }
  }

  /**
   * Starts the run: connects, joins the group (or applies the local manual selection) and starts
   * fetching. A manual run sends no membership request and arms neither the heartbeat nor any
   * membership timer.
   * A stop requested while the connection is still being made ends this start: it joins nothing.
   * @returns {Promise<void>} settles once the first round's apply is done and fetching is scheduled
   */
  async start() {
    if (this.running || this.stopRequested) {
      return
    }

    const { fence } = this.consumerGroup.startIdleWait()
    try {
      await this.consumerGroup.connect({ fence })
      // A stop that landed while connecting has retired this run: it must not start running.
      if (this.stopRequested) return
      // `running` is set BEFORE the first round: the round guard is `stopRequested`, which only
      // `stop()` sets, and guarding on `running` would have made the initial round dead code.
      this.running = true
      await this.runInitialRound()
    } catch (e) {
      this.running = false
      if (this.stopRequested && e?.type === 'CONSUMER_STOPPING') return

      try {
        return await this.onCrash(e)
      } catch (crashHandlerError) {
        // onCrash owns the CRASH event; if it still throws there is nothing left to notify
        // with, so at least make the failure visible instead of an unhandled rejection.
        this.logger.error('Crash handler failed', {
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
          error: crashHandlerError.message || crashHandlerError,
          cause: e.message || e,
        })
        return
      } finally {
        // A round that armed the manager before the chain threw leaves it running, and `stop()`
        // returns early on the `running` this catch has already cleared - so nothing else would
        // ever cancel it, and the handle is not unref'd. Latched rather than merely cancelled,
        // so a settling reconciliation cannot arm it again. Last word deliberately: it runs after
        // `onCrash` (and the disconnect/stop inside it), so a late re-arm is undone too.
        this.heartbeatManager.shutdown()
        this.pollTimer.stop()
      }
    }

    // A `stop()` that landed while the initial join was in flight has already cancelled the
    // manager and sent LeaveGroup. Re-arming here would keep heartbeating - and hold the event
    // loop open, since the handle is deliberately not unref'd - for a member that has left.
    if (this.stopRequested) {
      return
    }

    if (this.consumerGroup.kind !== 'manual') this.heartbeatManager.ensureStarted()
    this.scheduleFetchManager()
  }

  /**
   * The first assignment: a group round, or for a manual run the local apply, whose abandonment by
   * a stop is not a crash.
   * @private
   * @returns {Promise<void>}
   * @throws {Error} what the round or the local apply failed with
   */
  async runInitialRound() {
    if (this.consumerGroup.kind !== 'manual') {
      return this.rebalanceCoordinator.runRound(null, 'initial')
    }

    try {
      await this.consumerGroup.applyManualAssignment()
    } catch (error) {
      if (error?.type !== 'CONSUMER_STOPPING' || !this.stopRequested) throw error
    }
  }

  /**
   * Runs the fetch manager under the fetch retrier, one call per fetch loop, and chains the next
   * call when the retrier settles.
   *
   * Each attempt first takes a verdict that is already waiting in the slot and runs its round
   * instead of starting the fetch manager. A verdict can land while the loop sits between two
   * `start()` calls - in the retrier's backoff after a heartbeat failure that carried no verdict,
   * say - and nothing else reads the slot then: `start()` clears the stop latch the verdict set, so
   * a loop that started anyway would keep fetching for a member that has already left its group.
   *
   * An error from a membership a completed round already replaced is dropped after any waiting
   * verdict is handled, and the loop restarts instead of crashing the consumer. So does, for a
   * group member, a lost connection or a moved coordinator raised by one of the loop's own
   * requests (a Fetch round, a heartbeat, an automatic commit), after the retrier's first delay and
   * without spending a retry, until that class of request has failed for a session timeout with no
   * success (see `Outage`): the next round reconnects, and the heartbeat and the commit look the
   * coordinator up again. The ride runs the attempt again inside the same retrier attempt, so it
   * does not refill the retries that other errors have spent. A handler's errors, whatever they
   * are, count as before.
   * @returns {void}
   */
  scheduleFetchManager() {
    if (!this.running) {
      this.consuming = false

      this.logger.info('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      })

      return
    }

    this.consuming = true

    /**
     * One attempt of the fetch loop under the fetch retrier.
     * @param {(error: Error) => void} bail
     * @param {number} retryCount
     * @param {number} retryTime
     * @returns {Promise<true|undefined>} true when it rode out a lost connection: the retrier's
     *   attempt then runs it again, so a ride never refills the retries other errors have spent
     */
    const attempt = async (bail, retryCount, retryTime) => {
      if (!this.running) {
        return
      }

      /**
       * Runs one rebalance for the loop. A failed awaited callback ends the loop through `bail`, so
       * the retrier hands the typed error on unchanged instead of wrapping it.
       */
      const rebalance = async (verdict) => {
        try {
          await this.handleRebalanceError(verdict)
        } catch (error) {
          if (error instanceof KafkaJSRebalanceListenerError) return bail(error)
          throw error
        }
      }

      const waiting = this.rebalanceCoordinator.takeVerdict()

      if (waiting) {
        await rebalance(waiting)
        return
      }

      try {
        // The fetch manager (re)starting is the start of a fresh poll cycle: after a rebalance,
        // after a retry backoff, and on every loop of the retrier.
        this.pollTimer.reset()
        await this.fetchManager.start()
      } catch (e) {
        // A retired run's own work refusing to continue is the stop, not a crash: a crash here would
        // disconnect whatever run the consumer started since.
        if (e?.type === 'CONSUMER_STOPPING' && this.stopRequested) return

        // One rebalance must produce one REBALANCING and one join, even when the runner learns of
        // it twice - once as this throw, once as a slot entry. Merging and clearing first is what
        // guarantees that.
        const pending = this.rebalanceCoordinator.takeVerdict()
        const thrown = this.rebalanceCoordinator.classify(e, REBALANCE_PATHS.FETCH)
        const merged =
          thrown && pending ? this.mergeThrownWithSlot(thrown, pending) : thrown || pending

        if (merged) {
          await rebalance(merged)
          return
        }

        // An answer for a membership a completed round already replaced changes nothing.
        if (obsoleteOwnership(this.consumerGroup.ownership, e)) {
          this.logger.debug('Ignoring an error from a membership already replaced', {
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
            error: e.message,
          })
          return
        }

        const delay = this.rideOutage(e)
        if (delay !== undefined) {
          await sleep(delay)
          return true
        }

        if (e.name === 'KafkaJSNotImplemented' || e.name === 'KafkaJSNoBrokerAvailableError') {
          return bail(e)
        }

        this.logger.debug('Error while scheduling fetch manager, trying again...', {
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
          error: e.message,
          stack: e.stack,
          retryCount,
          retryTime,
        })

        throw e
      }

      const verdict = this.rebalanceCoordinator.takeVerdict()
      if (verdict) {
        await rebalance(verdict)
      }
    }

    this.fetchRetrier(async (bail, retryCount, retryTime) => {
      while (await attempt(bail, retryCount, retryTime));
    })
      .then(() => {
        this.scheduleFetchManager()
      })
      .catch(async (e) => {
        // consuming must be false before onCrash runs, or waitForConsumer() inside the
        // onCrash -> disconnect() -> stop() chain would await a CONSUMING_STOP that can only
        // be emitted after onCrash itself returns (deadlock). running, however, must stay
        // true until onCrash is done: onCrash's own stop() call needs it true to actually
        // stop the fetch manager and send LeaveGroup, instead of treating the runner as
        // already stopped and skipping both.
        this.consuming = false

        try {
          await this.onCrash(e)
        } catch (crashHandlerError) {
          // onCrash owns the CRASH event; if it still throws there is nothing left to notify
          // with, so at least make the failure visible instead of an unhandled rejection.
          this.logger.error('Crash handler failed', {
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
            error: crashHandlerError.message || crashHandlerError,
            cause: e.message || e,
          })
        } finally {
          this.running = false
          // A crashed runner is done: nothing will call `stop()` on it if the crash handler
          // decided not to, and its heartbeat handle is not unref'd, so it would keep the whole
          // cluster graph alive. A restart builds a new runner and arms its own.
          this.heartbeatManager.shutdown()
          this.pollTimer.stop()
        }
      })
  }

  /**
   * Merges an error the fetch manager threw with whatever the slot already held, under the
   * coordinator's LOST > REVOKED precedence. The slot entry is the pending one.
   * @param {import('./rebalanceCoordinator').Verdict} thrown
   * @param {import('./rebalanceCoordinator').Verdict} pending
   * @returns {import('./rebalanceCoordinator').Verdict}
   */
  mergeThrownWithSlot(thrown, pending) {
    return this.rebalanceCoordinator.mergeVerdict(pending, thrown)
  }

  /**
   * The one place a rebalance discards or auto-commits.
   *
   * LOST means the generation or member id the offsets belong to no longer exists, so committing
   * them would fail forever; REVOKED means the generation is still good and a final commit is
   * allowed. A fenced static member does both halves of LOST and then dies: rejoining under a
   * fresh member id would walk straight back into the same fencing loop.
   *
   * @param {Error} e
   * @returns {Promise<void>}
   * @throws {Error} `e` itself for a fenced member, and for anything that is not a rebalance
   */
  async handleRebalanceError(verdictOrError) {
    const verdict = this.toVerdict(verdictOrError)

    if (
      obsoleteOwnership(this.consumerGroup.ownership, verdict.error) ||
      this.rebalanceCoordinator.isStaleGenerationVerdict(verdict)
    ) {
      this.logger.debug('Ignoring a lost-membership answer for a generation already left', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        error: verdict.error.message,
      })

      return
    }

    const lost = verdict.kind === 'lost'
    if (lost) {
      const ownership = this.consumerGroup.ownership
      ownership?.lose(verdict.error, sentOwnership(verdict.error) ?? ownership.token())
      if (this.consumerGroup.kind === 'consumer') {
        this.consumerGroup.roundContext?.retire(verdict.error)
        await this.rebalanceCoordinator.idle()
      }
    }

    if (lost) {
      // The heartbeat response handler is the only thing that moves the membership state machine
      // on its own; a fencing learned on the commit or the fetch path has to be told. Idempotent,
      // because the same fencing can arrive on two paths at once. A no-op under classic.
      this.consumerGroup.onMembershipError(verdict.error)
    }

    const stopsTheWorld =
      lost || (verdict.kind === 'revoked' && this.consumerGroup.rebalanceProtocol() !== COOPERATIVE)

    if (!stopsTheWorld) {
      return this.resolveRebalanceVerdict(verdict)
    }

    if (this.rebalanceHooks) {
      // A listener's round prepares itself under its own budget: a loss dispatches the lost
      // callback without waiting for handlers, an eager revoke quiesces before its revoked callback.
      this.quiescing = lost
      try {
        return await this.resolveRebalanceVerdict({ ...verdict, membershipNotified: lost })
      } finally {
        this.quiescing = false
      }
    }

    // Java runs onJoinPrepare between polls, never under a running handler. The round below
    // replaces the offsets, so anything a handler resolves after that point lands in a manager
    // that is about to be discarded - and is re-delivered. Stopping the world first is what makes
    // the discard/auto-commit below see everything the handlers actually processed, and nothing
    // they had not.
    //
    // Neither await can wait on its own caller: `stop()` is latching and idempotent, and a
    // handler that threw the error being handled here has already returned.
    this.quiescing = true

    try {
      await this.fetchManager.stop()
      await this.waitForHandlers()

      return await this.resolveRebalanceVerdict({
        ...verdict,
        membershipNotified: lost,
      })
    } finally {
      this.quiescing = false
    }
  }

  /**
   * Normalises whatever reached `handleRebalanceError` into a verdict.
   *
   * A verdict raised by a path that knows why it is rebalancing - a stale assignment, say -
   * carries a `kind` and a `reason` the error itself does not have, and re-deriving them from the
   * error would lose both. An error arriving on its own is classified as it always was.
   *
   * @private
   * @param {import('./rebalanceCoordinator').Verdict|Error} input
   * @returns {{ kind: string|null, reason: string|null, error: Error }}
   */
  toVerdict(input) {
    if (input && input.kind && input.error) {
      return input
    }

    const classified = this.rebalanceCoordinator.classify(input, REBALANCE_PATHS.FETCH)

    return (
      classified || {
        kind: null,
        reason: rebalanceReason(input, { kind: this.consumerGroup.kind }),
        error: input,
      }
    )
  }

  /**
   * The verdict half of `handleRebalanceError`: discard or auto-commit, then rejoin.
   * @private
   * @param {{ kind: string|null, reason: string|null, error: Error }} verdict
   * @returns {Promise<void>}
   * @throws {Error} the verdict's error for a fenced member, and for anything that is not a
   *   rebalance
   */
  async resolveRebalanceVerdict({ kind, reason, error: e, membershipNotified = false }) {
    const ownership = this.consumerGroup.ownership
    if (obsoleteOwnership(ownership, e)) return
    if (kind === 'lost') {
      ownership?.lose(e, sentOwnership(e) ?? ownership.token())
      if (!membershipNotified) this.consumerGroup.onMembershipError(e)
      this.logger.error('Membership lost, discarding uncommitted offsets and re-joining', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        error: e.message,
      })

      this.consumerGroup.discardUncommittedOffsets()

      // A fatal membership error must never rejoin: an unsupported assignor, an invalid regex, a
      // duplicate instance id, a group at capacity, a revoked group ACL or a malformed request
      // would loop straight back into itself. The contract's own consumer-protocol set is used, so
      // the verdict's `fatal: true` and the runner's decision cannot disagree. Gated on the
      // consumer protocol, because a classic member's handling of the same codes is unchanged.
      if (
        isFencedInstance(e) ||
        (this.consumerGroup.kind === 'consumer' && isConsumerProtocolFatal(e))
      ) {
        ownership?.abandon(e, sentOwnership(e) ?? ownership.token())
        this.rebalanceCoordinator.abandon('lost')
        throw e
      }

      if (isUnknownMemberId(e)) {
        // Only an unrecognized member id needs a fresh one. A stale generation
        // (ILLEGAL_GENERATION) does not invalidate the member id itself, so keep it and avoid an
        // extra MEMBER_ID_REQUIRED round-trip.
        this.consumerGroup.forgetMemberId(sentMembership(e))
      }

      await this.rebalanceCoordinator.runRound(reason, 'lost')
      return
    }

    if (kind === 'revoked') {
      this.logger.warn('The group is rebalancing, re-joining', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        error: e.message,
      })

      try {
        if (!(this.rebalanceHooks && this.consumerGroup.rebalanceProtocol() !== COOPERATIVE)) {
          await this.autoCommitOffsets(null)
        }
      } catch (error) {
        const failure = this.rebalanceCoordinator.classify(error, REBALANCE_PATHS.OFFSET_COMMIT)
        if (failure?.kind === 'lost') return this.resolveRebalanceVerdict(failure)
        this.logger.warn('Pre-rebalance offset commit failed; rejoining anyway', {
          error: error.message,
        })
      }
      if (ownership?.refusal(ownership.token())) {
        return this.resolveRebalanceVerdict({
          kind: 'lost',
          reason: 'lost',
          error: ownership.refusal(ownership.token()),
        })
      }
      await this.rebalanceCoordinator.runRound(reason, 'revoked')
      return
    }

    throw e
  }

  /**
   * Stop the runner: halt fetching, wait for in-flight processing, then leave the group.
   *
   * By default, under the classic protocol, a static member (`groupInstanceId` set) keeps its
   * group slot across a normal shutdown (KIP-345: Java gates this the same way via
   * `AbstractCoordinator.shouldSendLeaveGroupRequest = LEAVE_GROUP || isDynamicMember()`), so no
   * LeaveGroup is sent for it. Under the consumer protocol a static member leaves by default with
   * the static leave epoch (-2), so the coordinator can hand its partitions to a returning
   * instance without waiting.
   *
   * `options.leaveGroup` overrides the default (KIP-1092): `true` removes the member from the
   * group at once, `false` keeps it there until its session expires, `undefined` is the default.
   * Every value but `undefined` goes to the group's own `leave`, which knows what it means for its
   * protocol.
   *
   * After the close callback a classic member makes one eligible final automatic commit, then
   * leaves. Every awaited stage runs under one operation: a close deadline ends the wait, skips
   * the stages that did not start and refuses the write of the ones that are queued. A stop
   * requested before the run started only latches, so that start joins nothing.
   *
   * @param {{ leaveGroup?: boolean }} [options]
   * @param {import('./groupProtocol/interface').ConsumerOperation} [operation] the close that owns
   *   this stop; its deadline and identity bound every stage, none when the stop waits naturally
   * @returns {Promise<void>} resolves also when the close deadline ended the stages
   */
  async stop(options, operation) {
    if (!this.running) {
      this.latchStop()
      return
    }

    this.logger.debug('stop consumer group', {
      groupId: this.consumerGroup.groupId,
      memberId: this.consumerGroup.memberId,
    })

    const closing = this.rebalanceHooks ? this.closeEpisode() : null
    this.latchStop()
    const scope = operation ?? new ConsumerOperation()

    try {
      // The fetch layer goes first: with `running` already false every fetch returns [] without
      // awaiting anything, so a fetcher left spinning would resolve in microtasks only and starve
      // the event loop the background round needs to make progress on the wire.
      await scope.wait(() => this.fetchManager.stop())
      // Fetch sessions close only once no fetcher can still send, and within this close's budget.
      await scope.wait(() =>
        this.consumerGroup.closeFetchSessions({
          deadlineAt: scope.deadlineAt,
          guard: scope.guard,
        })
      )
      // A background cooperative round must finish before LeaveGroup, or its JoinGroup would
      // re-register a member this runner has just left. Bounded by the current round, which is
      // the property waitForConsumer() already has. Nothing in that round re-arms the fetch
      // manager, so stopping it first cannot strand the round.
      await scope.wait(() => this.rebalanceCoordinator.idle())
      await scope.wait(() => this.waitForConsumer(scope))
      if (closing) await scope.wait(() => this.invokeCloseHook(closing, scope))

      await this.commitFinalOffsetsContained(scope)

      this.consumerGroup.clearAssignerState()

      // KIP-345's "a static member sends no LeaveGroup" is a CLASSIC rule. A KIP-848 member tells
      // the coordinator it is gone by default; the protocol picks epoch -1 or -2 from
      // `groupInstanceId`.
      // @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
      if (
        options?.leaveGroup === undefined &&
        this.consumerGroup.kind === 'classic' &&
        this.consumerGroup.groupInstanceId
      ) {
        this.consumerGroup.forgetMemberId()
      } else {
        await scope.wait(() =>
          this.consumerGroup.leave({ leaveGroup: options?.leaveGroup, operation: scope })
        )
      }
    } catch (e) {
      // The shutdown continues either way - the runner is already stopped by this point - but a
      // LeaveGroup that never reached the coordinator costs the whole session timeout before the
      // group reassigns, so it must not be swallowed silently. A close deadline is the caller's
      // own bound, reported by the close, not an error of the stop.
      if (!scope.guard()) {
        this.logger.error('Error while stopping the consumer group', {
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
          error: e.message || e,
        })
      }
    } finally {
      if (!operation) scope.finish()
    }
  }

  /**
   * The synchronous half of a stop, run once: refuses new batches, ends the inter-round yield and
   * the heartbeat and poll timers, retires the run's idle state and rounds and clears a waiting
   * verdict. A stop that finds no running run still latches, so a start in flight joins nothing.
   * @private
   * @returns {void}
   */
  latchStop() {
    if (this.stopLatched) return
    this.stopLatched = true
    this.stopRequested = true
    this.running = false
    // Ends the inter-round yield at once, so a chain waiting one out does not hold this stop open.
    this.rebalanceCoordinator.notifyStopRequested()
    // Before the manager is stopped, deliberately: a join waiting on its first heartbeat response
    // is only retried by the chain this next line cancels, so the rejection has to be in flight
    // first or `idle()` never resolves.
    this.consumerGroup.onStopRequested()
    // Latched, not merely cancelled: the rebalance chain awaited by `stop` can still reach
    // `requestTick()` through an acknowledgement or a coordinator move, and a chain armed there
    // would outlive LeaveGroup and hold the event loop open after `disconnect()` resolved.
    this.heartbeatManager.shutdown()
    this.pollTimer.stop()
    this.rebalanceCoordinator.clearVerdict()
  }

  /**
   * Retires this run for good when its close deadline passes: latches the stop and refuses every
   * later commit, heartbeat and hook wait of the run, so a handler or callback that outlives it
   * changes nothing. Also stops the fetch manager, which never rejects: the close's own stop never
   * reaches it once its deadline has passed, and with `running` false every fetch returns [] in
   * microtasks, so a fetcher left running would spin on `setImmediate` for the life of the process.
   * Synchronous and idempotent; application promises are left as they are.
   * @param {Error} error the first terminal reason
   * @returns {void}
   */
  retire(error) {
    if (this.retirement) return
    this.retirement = error
    this.latchStop()
    this.consumerGroup.retireCoordinatorLookup(error)
    this.consumerGroup.retireFetchSessions()
    this.fetchManager.stop()
    for (const check of this.hookWaiters || []) check()
  }

  /**
   * Captures, before the stop latches, the assignment a listener's close callback names. Whether
   * that callback is lost or revoked is decided only when it runs, after any round has settled.
   * @private
   * @returns {{ previous: Array<import('../../types').TopicPartitions> }}
   */
  closeEpisode() {
    return { previous: this.consumerGroup.assigned() }
  }

  /**
   * Calls the revoked callback for a close whose membership is intact, or the lost callback for one
   * whose membership is already lost, once and only for a nonempty assignment, before the final
   * commit and leave. A round still joining when the stop began does not make the close lost: the
   * stop waits for it, and Java reads its own rebalance state only after a poll has finished. The
   * callback shares the group's rebalance timeout and runs outside the stop requester's handler and
   * callback context; a failure is logged and the close goes on.
   * @private
   * @param {{ previous: Array<import('../../types').TopicPartitions> }} episode
   * @param {import('./groupProtocol/interface').ConsumerOperation} [operation] the close that owns
   *   the callback: its deadline shortens the rebalance timeout, its retirement expires the callback
   * @returns {Promise<void>} always resolves
   */
  async invokeCloseHook({ previous }, operation) {
    if (previous.length === 0) return
    const group = this.consumerGroup
    const { ownership } = group
    const lost = Boolean(ownership?.refusal(ownership.token()))
    const deadlineAt = Math.min(
      performance.now() + group.rebalanceTimeout,
      operation?.deadlineAt ?? Infinity
    )
    let retired = null
    const round = {
      run: {},
      roundDeadlineAt: deadlineAt,
      guard: () =>
        retired ||
        operation?.guard() ||
        (performance.now() >= deadlineAt ? consumerStoppingError() : null),
      retire: (error) => {
        retired ||= error
      },
      wait: (work) => work(),
    }
    try {
      // Whoever requested the stop, a handler or another callback, the close callback runs on its
      // own authority: it must neither inherit nor be refused by the requester's identity.
      await handlerContext.run(null, () =>
        RebalanceHooks.outside(() =>
          group.invokeHook(
            round,
            lost ? 'onPartitionsLost' : 'onPartitionsRevoked',
            previous,
            group.hookIdentity(round, previous)
          )
        )
      )
      const failure = group.takeListenerFailure(round)
      if (failure) throw failure
    } catch (error) {
      this.logger.error('Rebalance callback failed while closing', {
        groupId: group.groupId,
        memberId: group.memberId,
        error: describeThrown(error),
      })
    } finally {
      round.retire(consumerStoppingError())
      group.rebalanceHooks.retire({ run: round.run }, consumerStoppingError())
    }
  }

  /**
   * Resolves once the fetch loop has finished.
   * @param {import('./groupProtocol/interface').ConsumerOperation} [operation] the close that owns
   *   the wait: its retirement removes this wait's listener, so none outlives a forced close
   * @returns {Promise<void>}
   */
  waitForConsumer(operation) {
    return new Promise((resolve) => {
      if (!this.consuming) {
        return resolve()
      }

      this.logger.debug('waiting for consumer to finish...', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      })

      let unregister = () => {}
      const done = () => {
        unregister()
        this.off(CONSUMING_STOP, done)
        resolve()
      }
      this.on(CONSUMING_STOP, done)
      if (operation) unregister = operation.register(() => this.off(CONSUMING_STOP, done))
    })
  }

  /**
   * The user-facing heartbeat, shared with the timer through the group's own promise.
   *
   * It classifies and routes, and does no offset work: that belongs to `handleRebalanceError`,
   * which is reached once per rebalance. Under cooperative rebalancing a revocation RESOLVES
   * instead of throwing, so the error never reaches the worker, the fetcher or the fetch manager's
   * own teardown - which is the whole point of not stopping the world. It also ends the heartbeat
   * outage on a success, marks the error's request class as `heartbeat`, and resolves a cooperative
   * classic member's `NOT_COORDINATOR` while that outage is absorbed.
   * @returns {Promise<void>}
   */
  async heartbeat() {
    if (this.retirement) throw this.retirement
    try {
      await this.consumerGroup.heartbeat({
        interval: this.consumerGroup.heartbeatInterval(),
        guard: () => this.retirement ?? null,
      })
      this.outages.heartbeat.end()
    } catch (e) {
      const verdict = this.rebalanceCoordinator.classify(e, REBALANCE_PATHS.HEARTBEAT)

      if (verdict) {
        this.rebalanceCoordinator.requestRejoin(verdict)
      }

      if (this.consumerGroup.kind === 'consumer' && e.code === 29) return

      if (
        this.consumerGroup.rebalanceProtocol() === COOPERATIVE &&
        ((verdict && verdict.kind === 'revoked') ||
          (this.isClassicNotCoordinator(e) && this.outages.heartbeat.absorbs(e)))
      ) {
        return
      }

      throw this.markOwnRequest(e, 'heartbeat')
    }
  }

  /**
   * Whether a classic member's heartbeat was answered `NOT_COORDINATOR`. A cooperative classic
   * member's callback resolved on it when it was a revocation, and still does now that the member
   * looks the coordinator up again instead of rejoining, while its heartbeat outage lasts less than
   * the session timeout.
   * @private
   * @param {Error} error
   * @returns {boolean}
   */
  isClassicNotCoordinator(error) {
    return (this.consumerGroup.kind || 'classic') === 'classic' && error.code === NOT_COORDINATOR
  }

  /**
   * Which class of the fetch loop's own requests raised `error`.
   *
   * An OffsetCommit loading error propagated by a handler keeps its request provenance, so a
   * classic member retries a commit helper's failure within the commit outage. A handler-propagated
   * `NOT_COORDINATOR` (16) keeps it too: an OffsetCommit 16 rides in the commit class, any other 16
   * in the heartbeat class. Other handler errors keep their counted retries under both group protocols.
   * @private
   * @param {unknown} error
   * @returns {'fetch'|'heartbeat'|'commit'|undefined} undefined for other handler errors and for
   *   errors the loop's own requests did not raise
   */
  requestClassOf(error) {
    const requestClass = this.requestClasses.get(error)
    if (requestClass === 'handler') {
      if (error.code === NOT_COORDINATOR) return isOffsetCommitError(error) ? 'commit' : 'heartbeat'
      return error.code === COORDINATOR_LOAD_IN_PROGRESS_CODE && isOffsetCommitError(error)
        ? 'commit'
        : undefined
    }
    return requestClass ?? (isOffsetCommitError(error) ? 'commit' : undefined)
  }

  /**
   * Decides whether the loop rides `error` out as an outage of its own requests: the error's
   * request class is one the group protocol rides and its outage still absorbs it. The caller
   * waits the returned delay and tries again. Logs the warning the ride is reported with.
   * @private
   * @param {Error} error
   * @param {number} [nodeId] the failed Fetch broker; inferred from its error in the outer loop
   * @returns {number|undefined} the backoff delay in milliseconds; undefined when the error is not
   *   ridden out, which leaves it to the retrier's counted retries
   */
  rideOutage(error, nodeId = this.fetchNodes.get(error)) {
    const requestClass = this.requestClassOf(error)
    if (
      !requestClass ||
      !this.ridesOutages() ||
      !this.outages[requestClass].absorbs(error, performance.now(), nodeId)
    ) {
      return undefined
    }

    const delay = this.backoff.initial()
    this.logger.warn('Lost a connection while fetching, trying again', {
      groupId: this.consumerGroup.groupId,
      memberId: this.consumerGroup.memberId,
      error: error.message,
      requestClass,
      delay,
    })
    return delay
  }

  /**
   * Decides whether one node's failed Fetch stays on that node's fetcher. It does when the fetch
   * loop would ride the error out (`rideOutage`) and would not first turn it into a rebalance or
   * drop it as an answer for a replaced membership, or find a verdict already waiting. Everything
   * else reaches the loop through the fetch manager, as before. While no handler runs, the wait
   * counts as a poll, as the fetch manager's restart did, so an outage does not expire the poll
   * interval.
   * @private
   * @param {Error} error what a fetcher's Fetch threw
   * @param {number} nodeId the failed broker
   * @returns {number|undefined} the fetcher's backoff delay in milliseconds, or undefined
   */
  fetchRetryDelay(error, nodeId) {
    if (
      this.requestClassOf(error) !== 'fetch' ||
      this.rebalanceCoordinator.hasVerdict() ||
      this.rebalanceCoordinator.classify(error, REBALANCE_PATHS.FETCH) ||
      obsoleteOwnership(this.consumerGroup.ownership, error)
    ) {
      return undefined
    }

    const delay = this.rideOutage(error, nodeId)
    if (delay !== undefined && this.inFlightHandlers === 0) this.pollTimer.poll()
    return delay
  }

  /**
   * Whether the loop retries its own transport/coordinator outage without consuming retries.
   * Classic uses the configured session window. Consumer protocol has no client session window:
   * a learned fence, fatal error, poll expiry or explicit stop still ends or replaces membership.
   * @private
   * @returns {boolean}
   */
  ridesOutages() {
    const kind = this.consumerGroup.kind || 'classic'
    return kind === 'classic' || kind === 'consumer'
  }

  /**
   * Remembers which class of the fetch loop's own requests raised `error` (`fetch`, `heartbeat`),
   * or that it left through a handler (`handler`), so the loop rides out a lost connection only
   * where the client itself made the request. An OffsetCommit's error needs no mark: it carries
   * its own (`isOffsetCommitError`).
   * @private
   * @param {unknown} error
   * @param {'fetch'|'heartbeat'|'handler'} requestClass
   * @returns {unknown} the same error
   */
  markOwnRequest(error, requestClass) {
    if (error !== null && (typeof error === 'object' || typeof error === 'function')) {
      this.requestClasses.set(error, requestClass)
    }
    return error
  }

  async processEachMessage(batch) {
    const { topic, partition } = batch

    const pause = () => {
      this.consumerGroup.pause([{ topic, partitions: [partition] }])
      return () => this.consumerGroup.resume([{ topic, partitions: [partition] }])
    }
    for (const message of batch.messages) {
      if (
        !this.running ||
        this.consumerGroup.hasSeekOffset({ topic, partition }) ||
        this.isBatchStale(batch)
      ) {
        break
      }

      try {
        await this.eachMessage({
          topic,
          partition,
          message,
          heartbeat: () => this.heartbeat(),
          pause,
        })
      } catch (e) {
        this.markOwnRequest(e, 'handler')
        if (!isKafkaJSError(e)) {
          this.logger.error(`Error when calling eachMessage`, {
            topic,
            partition,
            offset: message.offset,
            stack: e.stack,
            error: e,
          })
        }

        // In case of errors, commit the previously consumed offsets unless autoCommit is disabled
        await this.commitAfterHandlerError(batch)
        throw e
      }

      this.resolveBatchOffset(
        batch,
        message.offset,
        message.batchContext ? message.batchContext.partitionLeaderEpoch : -1
      )

      // The offset stays resolved - an EAGER quiesce still commits it through
      // `resolveRebalanceVerdict` - but a membership dropped while this message ran cannot carry
      // a heartbeat or a commit of its own.
      if (this.isBatchStale(batch)) {
        break
      }

      await this.heartbeat()
      await this.autoCommitOffsetsIfNecessary()

      if (this.consumerGroup.isPaused(topic, partition)) {
        break
      }
    }
  }

  async processEachBatch(batch) {
    const { topic, partition } = batch
    const lastFilteredMessage = batch.messages[batch.messages.length - 1]

    const pause = () => {
      this.consumerGroup.pause([{ topic, partitions: [partition] }])
      return () => this.consumerGroup.resume([{ topic, partitions: [partition] }])
    }

    try {
      await this.eachBatch({
        batch,
        resolveOffset: (offset) => {
          /**
           * The transactional producer generates a control record after committing the transaction.
           * The control record is the last record on the RecordBatch, and it is filtered before it
           * reaches the eachBatch callback. When disabling auto-resolve, the user-land code won't
           * be able to resolve the control record offset, since it never reaches the callback,
           * causing stuck consumers as the consumer will never move the offset marker.
           *
           * When the last offset of the batch is resolved, we should automatically resolve
           * the control record offset as this entry doesn't have any meaning to the user-land code,
           * and won't interfere with the stream processing.
           *
           * @see https://github.com/apache/kafka/blob/9aa660786e46c1efbf5605a6a69136a1dac6edb9/clients/src/main/java/org/apache/kafka/clients/consumer/internals/Fetcher.java#L1499-L1505
           */
          const offsetToResolve =
            lastFilteredMessage && isSameOffset(offset, lastFilteredMessage.offset)
              ? batch.lastOffset()
              : offset

          this.resolveBatchOffset(batch, offsetToResolve, batch.leaderEpochAt(offsetToResolve))
        },
        heartbeat: () => this.heartbeat(),
        /**
         * Pause consumption for the current topic-partition being processed
         */
        pause,
        /**
         * Commit offsets if provided. Otherwise commit most recent resolved offsets
         * if the autoCommit conditions are met.
         *
         * @param {import('../../types').OffsetsByTopicPartition} [offsets] Optional.
         */
        commitOffsetsIfNecessary: async (offsets) => {
          if (offsets) {
            return this.commitOwnedOffsets(offsets, batch)
          }

          // The no-argument form commits on this batch's behalf, so it follows this batch's
          // partition: a handler still running for one a rebalance revoked must not move an
          // offset the new owner controls.
          if (
            this.isBatchStale(batch) ||
            this.consumerGroup.ownership?.refusal(this.consumerGroup.ownership.token())
          ) {
            this.logger.debug('Skipping a commit for a partition this member no longer holds', {
              groupId: this.consumerGroup.groupId,
              memberId: this.consumerGroup.memberId,
              topic,
              partition,
            })

            return
          }

          return this.consumerGroup.commitOffsetsIfNecessary({
            guard: () => this.commitRefusal(undefined, batch),
          })
        },
        uncommittedOffsets: () => this.consumerGroup.uncommittedOffsets(),
        isRunning: () => this.running,
        isStale: () => this.consumerGroup.hasSeekOffset({ topic, partition }),
      })
    } catch (e) {
      this.markOwnRequest(e, 'handler')
      if (!isKafkaJSError(e)) {
        this.logger.error(`Error when calling eachBatch`, {
          topic,
          partition,
          offset: batch.firstOffset(),
          stack: e.stack,
          error: e,
        })
      }

      // eachBatch has a special resolveOffset which can be used
      // to keep track of the messages
      await this.commitAfterHandlerError(batch)
      throw e
    }

    // resolveOffset for the last offset can be disabled to allow the users of eachBatch to
    // stop their consumers without resolving unprocessed offsets (issues/18)
    if (this.eachBatchAutoResolve) {
      const lastOffset = batch.lastOffset()
      this.resolveBatchOffset(batch, lastOffset, batch.leaderEpochAt(lastOffset))
    }
  }

  /**
   * Commits a caller-supplied offsets payload, dropping the partitions this member no longer
   * holds.
   *
   * `eachBatch`'s `commitOffsetsIfNecessary(offsets)` takes a full payload, which may name
   * partitions other than the batch's own - so ownership is decided per partition rather than
   * from the batch. A payload naming only partitions that were revoked commits nothing at all,
   * rather than committing an offset the new owner controls.
   *
   * A payload with no `topics` list means "commit what is resolved", which the offset manager
   * already keeps free of revoked partitions, so it passes through untouched.
   *
   * One attempt, with the broker's own error, and without the `running` skip of
   * `Runner#commitOffsets`: the final commit of a graceful shutdown still lands. The calling
   * batch fences it through the guard.
   *
   * @param {import('../../types').OffsetsByTopicPartition} offsets
   * @param {object} batch the calling handler's batch
   * @returns {Promise<void>}
   * @throws {KafkaJSCommitFailedError} when the batch or the membership no longer owns the commit
   * @throws {KafkaJSNonRetriableError} when this consumer has no group: an explicit payload is group-only
   */
  async commitOwnedOffsets(offsets, batch) {
    if (this.consumerGroup.groupId === undefined) {
      throw new KafkaJSNonRetriableError('Consumer groupId must be a non-empty string.')
    }
    const guard = () => this.commitRefusal(undefined, batch)
    const refusal = guard()
    if (refusal) throw refusal
    if (!Array.isArray(offsets.topics)) {
      return this.consumerGroup.commitOffsets(offsets, { guard })
    }

    const topics = offsets.topics
      .map((entry) => ({
        ...entry,
        partitions: (entry.partitions || []).filter(({ partition }) =>
          this.consumerGroup.isAssigned(entry.topic, partition)
        ),
      }))
      .filter(({ partitions }) => partitions.length > 0)

    if (topics.length === 0) {
      this.logger.debug('Skipping a commit for partitions this member no longer holds', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        topics: offsets.topics.map(({ topic }) => topic),
      })

      return
    }

    return this.consumerGroup.commitOffsets({ ...offsets, topics }, { guard })
  }

  /**
   * One fetch round against one broker.
   *
   * @param {number} nodeId
   * @returns {Promise<import('./batch')[]>}
   */
  async fetch(nodeId) {
    if (!this.running) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      })

      return []
    }

    const startFetch = Date.now()

    this.instrumentationEmitter.emit(FETCH_START, { nodeId })

    let batches
    try {
      batches = await this.consumerGroup.fetch(nodeId)
    } catch (e) {
      if (e && typeof e === 'object') this.fetchNodes.set(e, nodeId)
      throw this.markOwnRequest(e, 'fetch')
    }

    this.instrumentationEmitter.emit(FETCH, {
      /**
       * PR #570 removed support for the number of batches in this instrumentation event;
       * The new implementation uses an async generation to deliver the batches, which makes
       * this number impossible to get. The number is set to 0 to keep the event backward
       * compatible until we bump KafkaJS to version 2, following the end of node 8 LTS.
       *
       * @since 2019-11-29
       */
      numberOfBatches: 0,
      duration: Date.now() - startFetch,
      nodeId,
    })

    if (batches.length === 0) {
      // An in-flight Fetch can finish empty after stop retired its run. Starting a heartbeat
      // here would create a coordinator lookup after stop released the pending lookup's waiters,
      // holding fetchManager.stop() open through another lookup retry budget.
      if (!this.running) return batches

      // A round that produced nothing is still a poll: an idle consumer must not be evicted for
      // not having handed a handler anything.
      this.pollTimer.poll()
      await this.heartbeat()
    } else {
      this.outages.fetch.end(nodeId)
    }

    return batches
  }

  /**
   * Whether a batch has outlived the assignment it was fetched under.
   *
   * Four ways it can: the partition moved to another member, it stayed but the offsets were
   * replaced and the new assignment is already re-fetching the same records, the membership
   * itself was lost (a fence, an unknown member id, a poll-interval overrun), or its topic was
   * recreated, so the records belong to the old incarnation (`isRecreated`). Either way,
   * delivering it duplicates records and committing for it moves an offset this member no longer
   * controls. Checked when a batch is picked up AND between messages, because a rebalance can
   * land in the middle of a batch a handler is already working through. A lost membership stops
   * delivery the moment the loss is learned, before its round emits `PARTITIONS_LOST`, as the
   * Java client marks a fenced member's partitions pending revocation so that no buffered record
   * of theirs is returned.
   * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerMembershipManager.java
   *
   * @param {import('./batch')} batch
   * @returns {boolean}
   */
  isBatchStale(batch) {
    // An eager rebalance is about to replace the offsets, so a batch the worker picks up while it
    // is being handled would resolve into a manager that is already gone. Waiting for the
    // handlers to go idle is not enough on its own: the queue hands the worker the next batch the
    // instant the current one returns.
    if (this.quiescing) {
      return true
    }

    const ownership = this.consumerGroup.ownership
    if (ownership?.refusal(ownership.token())) {
      return true
    }

    if (
      batch.assignmentEpoch != null &&
      batch.assignmentEpoch !== this.consumerGroup.assignmentEpoch
    ) {
      return true
    }

    const acquired = this.consumerGroup.acquired?.get(
      batch.ownershipKey || `${batch.topic}|${batch.partition}`
    )
    if (
      batch.acquireClock != null &&
      acquired &&
      (acquired.acquiredAt > batch.acquireClock || acquired.recreatedAt > batch.acquireClock)
    )
      return true
    return !this.consumerGroup.isAssigned(batch.topic, batch.partition)
  }

  /**
   * Whether the batch's topic was deleted and recreated after the batch was fetched, so its
   * records belong to an incarnation this member no longer reads. Such a batch is stale for
   * delivery and commits (`isBatchStale`), and its offsets are never resolved
   * (`resolveBatchOffset`): they would name positions in a log that is not the new topic's. A
   * handler already running may finish; Java likewise discards fetched data whose topic id no
   * longer matches.
   *
   * @param {import('./batch')|object} batch a fetched batch or a handler's batch identity
   * @returns {boolean}
   */
  isRecreated(batch) {
    const acquired = this.consumerGroup.acquired?.get(
      batch.ownershipKey || `${batch.topic}|${batch.partition}`
    )
    return batch.acquireClock != null && acquired?.recreatedAt > batch.acquireClock
  }

  /**
   * Resolves the offset of a record a handler finished, unless the batch's topic was recreated
   * since the batch was fetched, in which case the offset is dropped.
   * @param {import('./batch')} batch the batch the record came from
   * @param {string} offset the handled record's offset
   * @param {number} leaderEpoch the epoch of the record at `offset`, -1 when none
   * @returns {void}
   */
  resolveBatchOffset(batch, offset, leaderEpoch) {
    if (this.isRecreated(batch)) return
    this.consumerGroup.resolveOffset({
      topic: batch.topic,
      partition: batch.partition,
      offset,
      leaderEpoch,
    })
  }

  /**
   * Runs one fetched batch through the user's handler. The handler's async context carries the
   * batch's ownership identity plus the identity of the run that delivered it, so a public API call
   * made later by that handler can be refused once the run is retired, even when the replacement
   * run's counters happen to equal the old ones.
   * @param {object} batch the fetched batch
   * @returns {Promise<void>}
   */
  async handleBatch(batch) {
    if (!this.running) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      })

      return
    }

    // Once per batch hand-off, before the handler. Deliberately not per message: one
    // `performance.now()` write per batch keeps the hottest path in the client untouched, at the
    // cost of measuring a very large batch as a whole.
    this.pollTimer.poll()

    if (this.isBatchStale(batch)) {
      this.logger.debug('Dropping a batch that outlived the assignment it was fetched under', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        topic: batch.topic,
        partition: batch.partition,
        batchAssignmentEpoch: batch.assignmentEpoch,
        assignmentEpoch: this.consumerGroup.assignmentEpoch,
      })

      return
    }

    // Admission only: a held key or an eager listener round drops queued work here and nowhere
    // else, so a handler that already runs for the key stays valid. Checked before any await and
    // in the same turn as the count below.
    if (
      this.rebalanceHooks &&
      (this.admissionQuiescing ||
        this.consumerGroup.subscriptionState.isHeld(batch.topic, batch.partition))
    ) {
      this.logger.debug('Dropping a queued batch for a partition held by a rebalance', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        topic: batch.topic,
        partition: batch.partition,
      })

      return
    }

    /** @param {import('./batch')} batch */
    const onBatch = async (batch) => {
      const startBatchProcess = Date.now()
      const payload = {
        topic: batch.topic,
        partition: batch.partition,
        highWatermark: batch.highWatermark,
        offsetLag: batch.offsetLag(),
        /**
         * @since 2019-06-24 (>= 1.8.0)
         *
         * offsetLag returns the lag based on the latest offset in the batch, to
         * keep the event backward compatible we just introduced "offsetLagLow"
         * which calculates the lag based on the first offset in the batch
         */
        offsetLagLow: batch.offsetLagLow(),
        batchSize: batch.messages.length,
        firstOffset: batch.firstOffset(),
        lastOffset: batch.lastOffset(),
      }

      if (this.logger.debugEnabled()) {
        this.logger.debug('Batch', {
          topic: batch.topic,
          partition: batch.partition,
          fetchedOffset: batch.fetchedOffset,
          rawMessages: batch.rawMessages.length,
          withinOffset: batch.messagesWithinOffset.length,
          messages: batch.messages.length,
          firstOffset: payload.firstOffset,
          lastOffset: payload.lastOffset,
          branch: batch.isEmptyDueToFiltering()
            ? 'empty-due-to-filtering'
            : batch.isEmpty()
              ? 'empty'
              : 'processed',
        })
      }

      /**
       * If the batch contained only control records or only aborted messages then we still
       * need to resolve and auto-commit to ensure the consumer can move forward.
       *
       * We also need to emit batch instrumentation events to allow any listeners keeping
       * track of offsets to know about the latest point of consumption.
       *
       * Added in #1256
       *
       * @see https://github.com/apache/kafka/blob/9aa660786e46c1efbf5605a6a69136a1dac6edb9/clients/src/main/java/org/apache/kafka/clients/consumer/internals/Fetcher.java#L1499-L1505
       */
      if (batch.isEmptyDueToFiltering()) {
        this.instrumentationEmitter.emit(START_BATCH_PROCESS, payload)

        const lastOffset = batch.lastOffset()
        this.resolveBatchOffset(batch, lastOffset, batch.leaderEpochAt(lastOffset))
        await this.autoCommitOffsetsIfNecessary(null)

        this.instrumentationEmitter.emit(END_BATCH_PROCESS, {
          ...payload,
          duration: Date.now() - startBatchProcess,
        })

        await this.heartbeat()
        return
      }

      if (batch.isEmpty()) {
        await this.heartbeat()
        return
      }

      this.instrumentationEmitter.emit(START_BATCH_PROCESS, payload)

      const { topic, partition, assignmentEpoch, acquireClock, ownershipKey } = batch
      const identity = {
        topic,
        partition,
        assignmentEpoch,
        acquireClock,
        ownershipKey,
        runIdentity: this.consumerGroup.runIdentity,
      }
      if (this.eachMessage) {
        await handlerContext.run(identity, () => this.processEachMessage(batch))
      } else if (this.eachBatch) {
        await handlerContext.run(identity, () => this.processEachBatch(batch))
      }

      this.instrumentationEmitter.emit(END_BATCH_PROCESS, {
        ...payload,
        duration: Date.now() - startBatchProcess,
      })

      // A rebalance can land while the handler runs - a poll-interval overrun drops the
      // membership outright - so the batch that was live on entry may be void by now. Committing
      // or heartbeating for it would use a generation this member no longer holds.
      if (this.isBatchStale(batch)) {
        this.logger.debug('Not committing a batch that outlived the assignment it ran under', {
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
          topic: batch.topic,
          partition: batch.partition,
          batchAssignmentEpoch: batch.assignmentEpoch,
          assignmentEpoch: this.consumerGroup.assignmentEpoch,
        })

        return
      }

      if (
        this.consumerGroup.offsetManager?.autoCommitInterval != null ||
        this.consumerGroup.offsetManager?.autoCommitThreshold != null
      ) {
        await this.autoCommitOffsetsIfNecessary(batch)
      } else {
        await this.autoCommitOffsets(batch)
      }
      this.outages.commit.end()
      await this.heartbeat()
    }

    this.inFlightHandlers++
    this.beginHookBatch(batch)

    try {
      await onBatch(batch)
    } finally {
      this.inFlightHandlers--
      this.endHookBatch(batch)

      if (this.inFlightHandlers === 0) {
        this.emit(HANDLERS_IDLE)
      }
    }
  }

  /**
   * Resolves once no handler is inside `eachMessage`/`eachBatch`.
   *
   * The same shape as `waitForConsumer()`, one level down: that one waits for the fetch loop to
   * finish, this one for the handlers it feeds. A handler that is itself the reason a rebalance
   * was raised has already returned by the time anything awaits this, so the wait cannot be on
   * the caller's own stack.
   *
   * @returns {Promise<void>}
   */
  waitForHandlers() {
    if (this.inFlightHandlers === 0) {
      return Promise.resolve()
    }

    this.logger.debug('waiting for in-flight handlers to return...', {
      groupId: this.consumerGroup.groupId,
      memberId: this.consumerGroup.memberId,
      inFlightHandlers: this.inFlightHandlers,
    })

    return new Promise((resolve) => this.once(HANDLERS_IDLE, () => resolve()))
  }

  /**
   * Counts a batch after its final admission check, only with a revoked callback.
   * @param {object} batch batch identity
   * @returns {void} no per-message bookkeeping
   */
  beginHookBatch(batch) {
    if (!this.rebalanceHooks?.has('onPartitionsRevoked')) return
    this.hookHandlers ||= new Map()
    const key = batch.ownershipKey || `${batch.topic}|${batch.partition}`
    this.hookHandlers.set(key, (this.hookHandlers.get(key) || 0) + 1)
  }

  /**
   * Finishes exactly the batch counted at admission, including failure and loss paths, and lets
   * every quiescence waiter re-check.
   * @param {object} batch batch identity
   * @returns {void} wakes current waiters synchronously
   */
  endHookBatch(batch) {
    if (this.hookHandlers) {
      const key = batch.ownershipKey || `${batch.topic}|${batch.partition}`
      const count = this.hookHandlers.get(key)
      if (count === 1) this.hookHandlers.delete(key)
      else if (count > 1) this.hookHandlers.set(key, count - 1)
    }
    if (this.hookWaiters) for (const check of this.hookWaiters) check()
  }

  /**
   * Awaits only the specified partitions' running handlers under the unchanged round deadline.
   * @param {Array<import('../../types').TopicPartitions>} partitions revoked keys
   * @param {object} options captured invocation boundary
   * @param {number} options.deadlineAt original absolute monotonic deadline
   * @param {object} options.identity original run and round; round supplies guard/wait/retire
   * @returns {Promise<void>} quiescent success, never expiry disguised as success
   * @throws {KafkaJSRebalanceListenerError|Error} typed expiry or owner retirement
   */
  waitForPartitions(partitions, { deadlineAt, identity }) {
    const keys = partitions.flatMap(({ topic, partitions }) =>
      partitions.map((partition) => `${topic}|${partition}`)
    )
    return this.awaitQuiescence(() => keys.some((key) => this.hookHandlers?.has(key)), {
      deadlineAt,
      identity,
    })
  }

  /**
   * Stops the fetch layer and awaits every running handler under the unchanged round deadline.
   * @param {object} options captured invocation boundary, as for `waitForPartitions`
   * @param {number} options.deadlineAt original absolute monotonic deadline
   * @param {object} options.identity original run and round
   * @returns {Promise<void>} quiescent success
   * @throws {KafkaJSRebalanceListenerError|Error} typed expiry or owner retirement
   */
  quiesceRun({ deadlineAt, identity }) {
    let stopped = false
    this.fetchManager.stop().then(() => {
      stopped = true
      for (const check of this.hookWaiters || []) check()
    })
    return this.awaitQuiescence(() => !stopped || this.inFlightHandlers > 0, {
      deadlineAt,
      identity,
    })
  }

  /**
   * A listener's eager preparation, run inside the round's own budget: admission stops, running
   * handlers finish, then the final automatic commit is attempted. A failed commit never blocks the
   * rebalance; a loss it reveals is reported to the caller, which then runs the lost callback.
   * @param {object} round the captured round
   * @returns {Promise<boolean>} whether the membership was lost during the preparation
   * @throws {KafkaJSRebalanceListenerError|Error} typed quiescence expiry or round retirement
   */
  async prepareEagerRound(round) {
    const ownership = this.consumerGroup.ownership
    this.admissionQuiescing = true
    await this.quiesceRun({
      deadlineAt: round.roundDeadlineAt,
      identity: { round, membership: ownership?.token() },
    })
    try {
      await round.wait(() => this.autoCommitOffsets(null, round))
    } catch (error) {
      const refusal = round.guard()
      if (refusal) throw refusal
      this.logger.warn('Pre-rebalance offset commit failed; rejoining anyway', {
        error: error.message,
      })
    }
    return Boolean(ownership?.refusal(ownership.token()))
  }

  /** Ends the admission-only stop of an eager listener round. @returns {void} */
  releaseEagerRound() {
    this.admissionQuiescing = false
  }

  /**
   * The shared waiter: resolves once `busy()` is false, rejects with the typed timeout at the
   * deadline (fencing the captured membership and retiring the round) and unlinks its own timer and
   * waiter on every exit. The promise is built lazily inside the round's wait, so a round that is
   * already retired never constructs it.
   * @private
   * @param {() => boolean} busy whether any watched handler still runs
   * @param {{ deadlineAt: number, identity: object }} options captured invocation boundary
   * @returns {Promise<void>} quiescent success
   * @throws {KafkaJSRebalanceListenerError|Error} typed expiry or owner retirement
   */
  async awaitQuiescence(busy, { deadlineAt, identity }) {
    const { round } = identity
    let timer, check, expired
    const timeout = () => {
      const cause = new Error('Revoked handlers did not quiesce within the rebalance deadline')
      return new KafkaJSRebalanceListenerError(cause.message, {
        hook: 'onPartitionsRevoked',
        reason: 'timeout',
        cause,
      })
    }
    const expire = () => {
      if (expired) return
      expired = timeout()
      this.consumerGroup.ownership?.lose(expired, identity.membership)
      round.retire(expired)
    }
    const guard = () => {
      if (!expired && performance.now() >= deadlineAt) expire()
      return expired || round.guard()
    }
    const before = guard()
    if (before) throw before
    if (!busy()) return
    this.hookWaiters ||= new Set()
    const pending = () =>
      new Promise((resolve, reject) => {
        check = () => {
          const refusal = guard()
          if (refusal) return reject(refusal)
          if (!busy()) resolve()
        }
        this.hookWaiters.add(check)
        timer = setTimeout(
          () => {
            const refusal = guard()
            if (refusal) reject(refusal)
            else check()
          },
          Math.max(0, Math.ceil(deadlineAt - performance.now()))
        )
        check()
      })
    try {
      await round.wait(pending)
      const after = guard()
      if (after) throw after
    } catch (error) {
      // The round's own timer can win the race by a rounding step: its expiry is this deadline.
      if (error?.type === 'JOIN_DEADLINE_EXCEEDED') expire()
      throw guard() || error
    } finally {
      clearTimeout(timer)
      this.hookWaiters.delete(check)
    }
  }

  /**
   * Checks the immutable operation identity and the calling handler's original batch.
   * @param {number} [token] the captured token; automatic calls default to current
   * @param {object} [batch] the captured batch; detached callers have no batch
   * @returns {Error|null} a refusal without writing or changing membership; a forced close's own
   *   reason once this run is retired
   */
  commitRefusal(token = this.consumerGroup.ownership?.token(), batch = handlerContext.current()) {
    if (this.retirement) return this.retirement
    const refusal = this.consumerGroup.ownership?.refusal(token)
    if (refusal) return refusal
    return batch && this.isBatchStale(batch)
      ? new KafkaJSCommitFailedError('The handler outlived its assignment', {
          reason: 'stale-assignment',
        })
      : null
  }

  /**
   * Best-effort automatic progress on a handler failure; preserves the handler's own error.
   * @param {object} batch the batch whose handler failed
   * @returns {Promise<void>}
   */
  async commitAfterHandlerError(batch) {
    if (this.isBatchStale(batch) || this.commitRefusal()) return
    try {
      await this.autoCommitOffsets()
    } catch (error) {
      this.logger.warn('Offset commit after a handler failure failed', {
        error: error.message,
      })
    }
  }

  /**
   * Commits the resolved offsets unless autoCommit is off or the batch no longer owns the commit.
   * @param {object|null} [batch] the batch committing; defaults to the calling handler's, `null`
   *   for none, as a commit outside any handler passes
   * @param {object|null} [round] the captured rebalance round the commit belongs to; its expiry or
   *   retirement refuses the commit before and after transmission
   * @returns {Promise<void>|undefined} undefined when nothing is attempted
   */
  autoCommitOffsets(batch = handlerContext.current() ?? null, round = null) {
    const refusal = () => round?.guard() || this.commitRefusal(undefined, batch)
    if (this.autoCommit && !refusal()) {
      return this.consumerGroup.commitOffsets({}, { auto: true, guard: refusal })
    }
  }

  /**
   * Commits the resolved offsets when the interval or threshold is due, unless autoCommit is off
   * or the batch no longer owns the commit. The guard keeps the batch it was created for, so a
   * transport that evaluates it later, in another async context, still checks the same batch.
   * @param {object|null} [batch] the batch committing; defaults to the calling handler's, `null`
   *   for none
   * @returns {Promise<void>|undefined} undefined when nothing is attempted
   */
  autoCommitOffsetsIfNecessary(batch = handlerContext.current() ?? null) {
    if (this.autoCommit && !this.commitRefusal(undefined, batch)) {
      return this.consumerGroup.commitOffsetsIfNecessary({
        guard: () => this.commitRefusal(undefined, batch),
      })
    }
  }

  /**
   * The close's one final attempt, contained: a failed commit is logged and the close goes on to
   * leave, but a retired close ends the stop at once.
   * @private
   * @param {import('./groupProtocol/interface').ConsumerOperation} operation the close scope
   * @returns {Promise<void>}
   * @throws {Error} the close's own refusal, when it expired during the attempt
   */
  async commitFinalOffsetsContained(operation) {
    try {
      await this.commitFinalOffsets(operation)
    } catch (error) {
      operation.check()
      this.logger.warn('Final automatic commit failed; leaving anyway', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        error: error.message || error,
      })
    }
  }

  /**
   * Attempts eligible classic progress once before leaving the captured closing run.
   * Consumer-protocol leave already owns its final attempt; manual behavior is unchanged.
   * @param {import('./groupProtocol/interface').ConsumerOperation} operation original close scope
   * @returns {Promise<void>} no attempt when automatic progress or ownership is ineligible
   * @throws {Error} commit failure or retirement; the close owner logs and continues cleanup
   */
  async commitFinalOffsets(operation) {
    const group = this.consumerGroup
    const manager = group.offsetManager
    const token = group.ownership?.token()
    if (
      !this.autoCommit ||
      !group.groupId ||
      group.kind !== 'classic' ||
      !manager ||
      operation.remaining() === 0 ||
      operation.guard() ||
      this.commitRefusal(token, null)
    )
      return
    if (manager.selectUncommitted().length === 0) return
    const guard = () =>
      operation.guard() ||
      (this.consumerGroup !== group || group.offsetManager !== manager
        ? new KafkaJSCommitFailedError('The closing assignment was replaced')
        : this.commitRefusal(token, null))
    await operation.wait(() => group.commitOffsets({}, { auto: true, guard, fence: operation }))
  }

  /**
   * Commits the given offsets for `consumer.commitOffsets()`, retrying through the commit retrier:
   * unlike the fetch loop's, it keeps the client's `retry.maxTotalTime`, so it rejects with
   * `KafkaJSDeliveryTimeout` once the deadline passes.
   *
   * With an API `operation` the whole call, first lookup included, runs under its one deadline: it
   * retries on the operation's own cancellable backoff and its guard refuses every later write. An
   * operation with no deadline keeps the ordinary retrier's schedule, as an unset `retry.maxTotalTime`
   * always did, on backoff timers the operation owns: its guard refuses writes once the call is
   * retired, and retirement removes a pending backoff timer and settles the call.
   * A commit made from a close callback runs while the stop is in progress; any other call
   * resolves at once, committing nothing, once the runner is no longer running.
   * A commit answered with a rebalance in progress still rejects with that answer, and also asks
   * the group to rejoin, as the same answer to a heartbeat does.
   * @param {Object} offsets the offsets to commit
   * @param {object} [batch] the original handler batch, absent for detached calls
   * @param {{ operation?: import('./groupProtocol/interface').ConsumerOperation }} [context] the
   *   public call's private deadline scope
   * @returns {Promise<void>} resolves at once, committing nothing, when the runner is not running
   * @throws {import('../errors').KafkaJSDeliveryTimeout|import('../errors').KafkaJSNumberOfRetriesExceeded|Error}
   */
  commitOffsets(offsets, batch = handlerContext.current() ?? null, { operation } = {}) {
    const ownership = this.consumerGroup.ownership
    // A commit from a rebalance callback keeps that invocation's membership, expiry and original
    // acquisition cells through its retries and the sender; it never adopts what is current at
    // transmission.
    const invocation = RebalanceHooks.current()
    const token = invocation ? invocation.context.membership : ownership?.token()
    const release = ownership?.retain(token)
    // A close callback commits while the stop runs; its invocation guard still expires it.
    const closing = Boolean(invocation) && this.stopRequested
    const authority = () =>
      RebalanceHooks.refusal(invocation) ||
      this.commitRefusal(token, batch) ||
      (invocation && this.consumerGroup.hookCommitRefusal(invocation.context, offsets.topics)) ||
      null
    // An expired call's deadline stays its caller-facing error but never masks a later stop or
    // retirement: the run's authority is checked first, so a retired run publishes nothing.
    const guard = () => {
      const call = operation?.guard()
      if (!call) return authority()
      if (!isCallDeadline(call)) return call
      return (!this.running && !closing && consumerStoppingError()) || authority() || call
    }
    if (!this.running && !closing) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        offsets,
      })
      release?.()
      return
    }

    const retrier =
      operation && operation.deadlineAt !== Infinity
        ? (attempt) => operation.retry(attempt, this.retryOptions)
        : (attempt) => this.commitRetrier(attempt, operation?.retryScope())
    return retrier(async (bail, retryCount, retryTime) => {
      try {
        const refusal = guard()
        if (refusal) return bail(refusal)
        await this.consumerGroup.commitOffsets(offsets, {
          guard,
          ...(operation && { fence: operation }),
        })
      } catch (e) {
        if (e instanceof KafkaJSCommitFailedError) return bail(e)
        if (!this.running && !closing) {
          this.logger.debug('consumer not running, exiting', {
            error: e.message,
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
            offsets,
          })
          return
        }

        if (e.name === 'KafkaJSNotImplemented') {
          return bail(e)
        }

        const verdict = this.rebalanceCoordinator.classify(e, REBALANCE_PATHS.OFFSET_COMMIT)
        if (verdict?.kind === 'revoked') this.rebalanceCoordinator.requestRejoin(verdict)

        this.logger.debug('Error while committing offsets, trying again...', {
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
          error: e.message,
          stack: e.stack,
          retryCount,
          retryTime,
          offsets,
        })

        throw e
      }
    }).finally(() => release?.())
  }
}
