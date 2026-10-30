const { EventEmitter } = require('events')
const Long = require('../utils/long')
const createRetry = require('../retry')
const sleep = require('../utils/sleep')
const {
  isKafkaJSError,
  isFencedInstance,
  isUnknownMemberId,
  KafkaJSCommitFailedError,
  COORDINATOR_LOAD_IN_PROGRESS_CODE,
} = require('../errors')
const handlerContext = require('./handlerContext')

const {
  events: { FETCH, FETCH_START, START_BATCH_PROCESS, END_BATCH_PROCESS },
} = require('./instrumentationEvents')
const createFetchManager = require('./fetchManager')
const RebalanceCoordinator = require('./rebalanceCoordinator')
const PollTimer = require('./pollTimer')
const Outage = require('./outage')
const HeartbeatManager = require('./heartbeatManager')
const {
  REBALANCE_PATHS,
  isOffsetCommitError,
  rebalanceReason,
  isConsumerProtocolFatal,
  sentOwnership,
  sentMembership,
  obsoleteOwnership,
} = require('./rebalanceErrorContract')
const { COOPERATIVE } = require('./groupProtocol/interface')

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
    this.backoff = createRetry.backoff(retry)
    /**
     * The outages a classic member's fetch loop rides out, one per class of the client's own
     * requests in it: each ends only when a request of its own class succeeds.
     */
    this.outages = {
      fetch: new Outage(sessionTimeout),
      heartbeat: new Outage(sessionTimeout),
      commit: new Outage(sessionTimeout),
    }
    /** @type {WeakMap<object, 'fetch'|'heartbeat'|'handler'>} where an error left the loop's work */
    this.requestClasses = new WeakMap()
    this.onCrash = onCrash
    this.autoCommit = autoCommit
    this.maxPrefetchBatches = maxPrefetchBatches
    this.fetchManager = createFetchManager({
      logger: this.logger,
      getNodeIds: () => this.consumerGroup.getNodeIds(),
      fetch: (nodeId) => this.fetch(nodeId),
      handler: (batch) => this.handleBatch(batch),
      concurrency,
    })

    this.running = false
    this.consuming = false
    this.stopRequested = false
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

  async start() {
    if (this.running) {
      return
    }

    this.consumerGroup.startIdleWait()
    try {
      await this.consumerGroup.connect()
      // `running` is set BEFORE the first round: the round guard is `stopRequested`, which only
      // `stop()` sets, and guarding on `running` would have made the initial round dead code.
      this.running = true
      await this.rebalanceCoordinator.runRound(null, 'initial')
    } catch (e) {
      this.running = false

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

    this.heartbeatManager.ensureStarted()
    this.scheduleFetchManager()
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
   * classic member, a lost connection or a moved coordinator raised by one of the loop's own
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

      const waiting = this.rebalanceCoordinator.takeVerdict()

      if (waiting) {
        await this.handleRebalanceError(waiting)
        return
      }

      try {
        // The fetch manager (re)starting is the start of a fresh poll cycle: after a rebalance,
        // after a retry backoff, and on every loop of the retrier.
        this.pollTimer.reset()
        await this.fetchManager.start()
      } catch (e) {
        // One rebalance must produce one REBALANCING and one join, even when the runner learns of
        // it twice - once as this throw, once as a slot entry. Merging and clearing first is what
        // guarantees that.
        const pending = this.rebalanceCoordinator.takeVerdict()
        const thrown = this.rebalanceCoordinator.classify(e, REBALANCE_PATHS.FETCH)
        const merged =
          thrown && pending ? this.mergeThrownWithSlot(thrown, pending) : thrown || pending

        if (merged) {
          await this.handleRebalanceError(merged)
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

        const requestClass = this.requestClassOf(e)
        if (requestClass && this.ridesOutages() && this.outages[requestClass].absorbs(e)) {
          const delay = this.backoff.initial()
          this.logger.warn('Lost a connection while fetching, trying again', {
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
            error: e.message,
            requestClass,
            delay,
          })
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
        await this.handleRebalanceError(verdict)
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
        await this.autoCommitOffsets(null)
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
   * @param {{ leaveGroup?: boolean }} [options]
   * @returns {Promise<void>}
   */
  async stop(options) {
    if (!this.running) {
      return
    }

    this.logger.debug('stop consumer group', {
      groupId: this.consumerGroup.groupId,
      memberId: this.consumerGroup.memberId,
    })

    this.stopRequested = true
    this.running = false
    // Ends the inter-round yield at once, so a chain waiting one out does not hold this stop open.
    this.rebalanceCoordinator.notifyStopRequested()
    // Before the manager is stopped, deliberately: a join waiting on its first heartbeat response
    // is only retried by the chain this next line cancels, so the rejection has to be in flight
    // first or `idle()` below never resolves.
    this.consumerGroup.onStopRequested()
    // Latched, not merely cancelled: the rebalance chain awaited below can still reach
    // `requestTick()` through an acknowledgement or a coordinator move, and a chain armed there
    // would outlive LeaveGroup and hold the event loop open after `disconnect()` resolved.
    this.heartbeatManager.shutdown()
    this.pollTimer.stop()
    this.rebalanceCoordinator.clearVerdict()

    try {
      // The fetch layer goes first: with `running` already false every fetch returns [] without
      // awaiting anything, so a fetcher left spinning would resolve in microtasks only and starve
      // the event loop the background round needs to make progress on the wire.
      await this.fetchManager.stop()
      // A background cooperative round must finish before LeaveGroup, or its JoinGroup would
      // re-register a member this runner has just left. Bounded by the current round, which is
      // the property waitForConsumer() already has. Nothing in that round re-arms the fetch
      // manager, so stopping it first cannot strand the round.
      await this.rebalanceCoordinator.idle()
      await this.waitForConsumer()

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
        await this.consumerGroup.leave(options)
      }
    } catch (e) {
      // The shutdown continues either way - the runner is already stopped by this point - but a
      // LeaveGroup that never reached the coordinator costs the whole session timeout before the
      // group reassigns, so it must not be swallowed silently.
      this.logger.error('Error while stopping the consumer group', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        error: e.message || e,
      })
    }
  }

  waitForConsumer() {
    return new Promise((resolve) => {
      if (!this.consuming) {
        return resolve()
      }

      this.logger.debug('waiting for consumer to finish...', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      })

      this.once(CONSUMING_STOP, () => resolve())
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
    try {
      await this.consumerGroup.heartbeat({
        interval: this.consumerGroup.heartbeatInterval(),
      })
      this.outages.heartbeat.end()
    } catch (e) {
      const verdict = this.rebalanceCoordinator.classify(e, REBALANCE_PATHS.HEARTBEAT)

      if (verdict) {
        this.rebalanceCoordinator.requestRejoin(verdict)
      }

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
   * in the heartbeat class. Other handler errors keep their counted retries; consumer-protocol
   * members do not ride outages.
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
   * Whether this member's fetch loop rides out lost connections: a classic member's does. A
   * consumer-protocol member keeps its counted retries.
   * @private
   * @returns {boolean}
   */
  ridesOutages() {
    return (this.consumerGroup.kind || 'classic') === 'classic'
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

      this.consumerGroup.resolveOffset({
        topic,
        partition,
        offset: message.offset,
        leaderEpoch: message.batchContext ? message.batchContext.partitionLeaderEpoch : -1,
      })

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

          this.consumerGroup.resolveOffset({
            topic,
            partition,
            offset: offsetToResolve,
            leaderEpoch: batch.leaderEpochAt(offsetToResolve),
          })
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
      this.consumerGroup.resolveOffset({
        topic,
        partition,
        offset: lastOffset,
        leaderEpoch: batch.leaderEpochAt(lastOffset),
      })
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
   */
  async commitOwnedOffsets(offsets, batch) {
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
      // A round that produced nothing is still a poll: an idle consumer must not be evicted for
      // not having handed a handler anything.
      this.pollTimer.poll()
      await this.heartbeat()
    } else {
      // A leader answered: the fetch outage the loop was riding out, if any, is over.
      this.outages.fetch.end()
    }

    return batches
  }

  /**
   * Whether a batch has outlived the assignment it was fetched under.
   *
   * Three ways it can: the partition moved to another member, it stayed but the offsets were
   * replaced and the new assignment is already re-fetching the same records, or the membership
   * itself was lost (a fence, an unknown member id, a poll-interval overrun). Either way,
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
    if (batch.acquireClock != null && acquired && acquired.acquiredAt > batch.acquireClock)
      return true
    return !this.consumerGroup.isAssigned(batch.topic, batch.partition)
  }

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
        this.consumerGroup.resolveOffset({
          topic: batch.topic,
          partition: batch.partition,
          offset: lastOffset,
          leaderEpoch: batch.leaderEpochAt(lastOffset),
        })
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

      await this.autoCommitOffsets(batch)
      this.outages.commit.end()
      await this.heartbeat()
    }

    this.inFlightHandlers++

    try {
      await onBatch(batch)
    } finally {
      this.inFlightHandlers--

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
   * Checks the immutable operation identity and the calling handler's original batch.
   * @param {number} [token] the captured token; automatic calls default to current
   * @param {object} [batch] the captured batch; detached callers have no batch
   * @returns {KafkaJSCommitFailedError|null} a refusal without writing or changing membership
   */
  commitRefusal(token = this.consumerGroup.ownership?.token(), batch = handlerContext.current()) {
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
   * @returns {Promise<void>|undefined} undefined when nothing is attempted
   */
  autoCommitOffsets(batch = handlerContext.current() ?? null) {
    if (this.autoCommit && !this.commitRefusal(undefined, batch)) {
      const guard = () => this.commitRefusal(undefined, batch)
      return this.consumerGroup.commitOffsets({}, { auto: true, guard })
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
   * Commits the given offsets for `consumer.commitOffsets()`, retrying through the commit retrier:
   * unlike the fetch loop's, it keeps the client's `retry.maxTotalTime`, so it rejects with
   * `KafkaJSDeliveryTimeout` once the deadline passes.
   * @param {Object} offsets the offsets to commit
   * @param {object} [batch] the original handler batch, absent for detached calls
   * @returns {Promise<void>} resolves at once, committing nothing, when the runner is not running
   * @throws {import('../errors').KafkaJSDeliveryTimeout|import('../errors').KafkaJSNumberOfRetriesExceeded|Error}
   */
  commitOffsets(offsets, batch = handlerContext.current() ?? null) {
    const ownership = this.consumerGroup.ownership
    const token = ownership?.token()
    const release = ownership?.retain(token)
    const guard = () => this.commitRefusal(token, batch)
    if (!this.running) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        offsets,
      })
      release?.()
      return
    }

    return this.commitRetrier(async (bail, retryCount, retryTime) => {
      try {
        const refusal = guard()
        if (refusal) return bail(refusal)
        await this.consumerGroup.commitOffsets(offsets, { guard })
      } catch (e) {
        if (e instanceof KafkaJSCommitFailedError) return bail(e)
        if (!this.running) {
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
