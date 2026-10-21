const describeThrown = require('../network/describeThrown')
const { KafkaJSNonRetriableError, isFencedInstance, isUnknownMemberId } = require('../errors')
const {
  rebalanceAction,
  rebalanceReason,
  pollIntervalExceededError,
  sentGeneration,
  sentOwnership,
  stampOwnership,
  obsoleteOwnership,
  learnOwnershipLoss,
} = require('./rebalanceErrorContract')
const { COOPERATIVE } = require('./groupProtocol/interface')
const {
  events: {
    REBALANCING,
    GROUP_JOIN,
    PARTITIONS_ASSIGNED,
    PARTITIONS_REVOKED,
    PARTITIONS_LOST,
    POLL_INTERVAL_EXCEEDED,
  },
} = require('./instrumentationEvents')

/**
 * Consecutive rounds one chain may run before it is treated as non-converging. A cooperative
 * round that revokes something runs a follow-up round; a pair of members that keep taking
 * partitions from each other would otherwise chain forever.
 */
const MAX_REJOIN_ROUNDS = 20

/**
 * The longest a chain waits between two rounds of one reconciliation. Matches Kafka's default
 * `group.consumer.heartbeat.interval.ms`: the yield exists to space coordinator trips, not to sleep
 * out a backoff - and the membership's interval IS the backoff remainder while one is armed, which
 * is capped at `retry.maxRetryTime`.
 * @see https://kafka.apache.org/documentation/#consumerconfigs
 */
const MAX_RECONCILE_YIELD_MS = 5000

/**
 * The `type` of the internal error the consumer group protocol rejects its pending join with when
 * the runner asks it to stop. Matched by type rather than by class, as the member-id branch of
 * `ConsumerGroup#joinAndSync` matches `UNKNOWN_MEMBER_ID`.
 */
const CONSUMER_STOPPING = 'CONSUMER_STOPPING'

/**
 * The second of the two codes a coordinator uses to stop recognising a consumer-protocol member.
 * The first, `UNKNOWN_MEMBER_ID`, already has an exported predicate.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java
 */
const FENCED_MEMBER_EPOCH_CODE = 110

/** The Java client's message, adapted to this client's option names. */
const POLL_TIMEOUT_WARNING =
  'consumer poll timeout has expired. This means the time between subsequent calls to your ' +
  'eachMessage/eachBatch handler was longer than the configured maxPollInterval, which typically ' +
  'implies that the handler is spending too much time processing messages. Increase ' +
  'maxPollInterval, or reduce the batch size.'

/**
 * @typedef {'lost' | 'revoked'} RebalanceAction the two verdict kinds a rebalance can produce
 *
 * @typedef {Object} Verdict
 * @property {RebalanceAction} kind whether the generation is gone (`lost`) or still good
 * @property {string} reason the `REBALANCING` reason this verdict reports
 * @property {Error} error always error-shaped, because every consumer of a verdict is
 */

/**
 * The single owner of every rebalance.
 *
 * It is the only caller of `consumerGroup.joinAndSync()`, the only emitter of `REBALANCING`,
 * `GROUP_JOIN`, the three partition events and `POLL_INTERVAL_EXCEEDED`, the only holder of the
 * round loop, the round counter, the mid-chain rejoin request and the pending-verdict slot, and
 * the only owner of the poll-expiry sequence.
 *
 * A **round** is one JoinGroup plus one SyncGroup plus the apply, and rounds run in a
 * single-flight chain: a trigger that arrives while a chain runs is remembered, never raced.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 */
module.exports = class RebalanceCoordinator {
  /**
   * @param {object} options
   * @param {import('./runner')} options.runner read for `stopRequested` and `onCrash`
   * @param {import('./consumerGroup')} options.consumerGroup
   * @param {ReturnType<typeof import('./fetchManager')>} options.fetchManager `fetchManager.js`
   *   exports a factory, not a class
   * @param {import('./heartbeatManager')} options.heartbeatManager
   * @param {import('./pollTimer')} options.pollTimer
   * @param {import('../instrumentation/emitter')} options.instrumentationEmitter
   * @param {import('../../types').Logger} options.logger
   * @param {string} options.groupId
   * @param {() => void} [options.onJoinComplete] called once per completed round, immediately
   *   before `GROUP_JOIN` is delivered, so state it records is visible to every observer of that
   *   event whatever the order public instrumentation reaches them in
   */
  constructor({
    runner,
    consumerGroup,
    fetchManager,
    heartbeatManager,
    pollTimer,
    instrumentationEmitter,
    logger,
    groupId,
    onJoinComplete = () => {},
  }) {
    this.runner = runner
    this.consumerGroup = consumerGroup
    this.fetchManager = fetchManager
    this.heartbeatManager = heartbeatManager
    this.pollTimer = pollTimer
    this.instrumentationEmitter = instrumentationEmitter
    this.logger = logger.namespace('RebalanceCoordinator')
    this.groupId = groupId
    this.onJoinComplete = onJoinComplete

    /** @type {Promise<void>|null} the running chain; also the single-flight marker */
    this.inFlight = null
    /** @type {string|null} a reason a trigger left behind while a chain was running */
    this.rejoinRequested = null
    /** @type {'lost'|null} set when the trigger left behind was a LOST */
    this.pendingKind = null
    /** @type {number|null} the group's `joinCount` when that LOST was left behind */
    this.pendingJoinCount = null
    /** @type {Verdict|null} the aborting-path slot */
    this.verdict = null
    /** Consecutive rounds in the current chain. */
    this.rounds = 0
    /**
     * Callbacks that end a wait a stop has made pointless, armed by `yieldBetweenRounds` and
     * fired by `notifyStopRequested`.
     * @type {Set<() => void>}
     */
    this.stopWaiters = new Set()
  }

  /**
   * Ends every wait that a requested stop makes pointless.
   *
   * Called by `Runner#stop()` as it sets `stopRequested`. Without it the inter-round yield below
   * runs to its full delay - up to `MAX_RECONCILE_YIELD_MS` - inside the chain `stop()` then
   * awaits through `idle()`, so `consumer.disconnect()` would wait for a pause that no longer has
   * a next round to space out.
   * @returns {void}
   */
  notifyStopRequested() {
    for (const waiter of [...this.stopWaiters]) {
      waiter()
    }
  }

  /**
   * One coordinator trip's worth of spacing between two consumer-protocol reconciliation rounds,
   * so a multi-trip reconciliation spends its round budget on real trips instead of spinning the
   * event loop.
   *
   * Raced against the stop signal rather than merely guarded before it: a stop landing one tick
   * after the timer is armed would otherwise hold `stop()` open for the whole delay. Deliberately
   * not `unref()`ed - the chain awaits this timer, so an unreferenced one would strand the round
   * whenever nothing else keeps the loop alive.
   * @private
   * @returns {Promise<void>}
   */
  yieldBetweenRounds() {
    const delay = Math.min(this.consumerGroup.heartbeatInterval(), MAX_RECONCILE_YIELD_MS)

    return new Promise((resolve) => {
      let timer = null
      const finish = () => {
        clearTimeout(timer)
        this.stopWaiters.delete(finish)
        resolve()
      }

      timer = setTimeout(finish, delay)
      this.stopWaiters.add(finish)
    })
  }

  /** @returns {boolean} whether a round is running right now */
  isRebalancing() {
    return this.inFlight !== null
  }

  /**
   * Runs a rebalance round, and any follow-up rounds it turns out to need, as one chain.
   *
   * Re-entering while a chain runs does NOT start a second one: the reason is remembered and the
   * running chain picks it up when it settles. A LOST re-entry additionally carries its kind, so
   * the follow-up round emits `PARTITIONS_LOST` and reports its own reason rather than inheriting
   * the earlier round's.
   *
   * @param {string|null} reason null only for the first round of a run, which emits no
   *   `REBALANCING`
   * @param {'lost'|'revoked'|'initial'} kind
   * @returns {Promise<void>} the chain; the same promise for a re-entrant call
   */
  runRound(reason, kind) {
    if (this.inFlight) {
      if (kind === 'lost') {
        this.pendingKind = 'lost'
        this.pendingJoinCount = this.consumerGroup.joinCount
        this.rejoinRequested = reason
      } else {
        this.rejoinRequested = this.mergeReason(this.rejoinRequested, reason)
      }

      return this.inFlight
    }

    this.inFlight = this.runChain(reason, kind).finally(() => {
      this.inFlight = null
      this.rounds = 0
      this.pendingKind = null
      this.pendingJoinCount = null
      // A chain that threw must not hand its pending reason to the next one: the trigger belonged
      // to the membership that chain was replacing, and the next chain would run a round for it.
      this.rejoinRequested = null
      // A chain that threw must not leave stale-assignment detection off for the life of the
      // group: the latch is cleared here as well as by a successful apply.
      this.consumerGroup.clearStaleAssignmentPending()
    })

    return this.inFlight
  }

  /**
   * One rebalance chain, with the shutdown rule around it.
   *
   * A join cancelled because the consumer is shutting down is not a crash: `Runner#stop()` rejects
   * the pending join deliberately, so the chain settles and `idle()` resolves. Any other error, and
   * this same error outside a shutdown, still reaches the caller - whose only sink is the
   * consumer's crash handler.
   * @private
   * @param {string|null} reason
   * @param {'lost'|'revoked'|'initial'} kind
   * @returns {Promise<void>}
   * @throws {KafkaJSNonRetriableError} when the chain does not converge
   */
  async runChain(reason, kind) {
    try {
      await this.runRounds(reason, kind)
    } catch (error) {
      if (error && error.type === CONSUMER_STOPPING && this.runner.stopRequested) {
        this.logger.debug('Rebalance chain abandoned because the consumer is stopping', {
          groupId: this.groupId,
          memberId: this.consumerGroup.memberId,
        })

        return
      }

      throw error
    }
  }

  /**
   * The rounds of one chain. Called only by `runChain`, which owns the shutdown rule.
   * @private
   * @param {string|null} reason
   * @param {'lost'|'revoked'|'initial'} kind
   * @returns {Promise<void>}
   * @throws {KafkaJSNonRetriableError} when the chain does not converge
   */
  async runRounds(reason, kind) {
    do {
      if (this.runner.stopRequested) {
        return
      }

      if (++this.rounds > MAX_REJOIN_ROUNDS) {
        throw new KafkaJSNonRetriableError(
          `cooperative rebalance did not converge in ${MAX_REJOIN_ROUNDS} rounds`
        )
      }

      if (reason) {
        this.instrumentationEmitter.emit(REBALANCING, {
          groupId: this.groupId,
          memberId: this.consumerGroup.memberId,
          membershipProtocol: this.consumerGroup.kind || 'classic',
          reason,
        })
      }

      const previous = this.consumerGroup.assigned()
      const cooperative = this.consumerGroup.rebalanceProtocol() === COOPERATIVE
      // A round that gives everything up rebuilds from scratch: an eager round revoked its whole
      // assignment before joining, and a lost one held its under a membership that is now void,
      // so the incremental apply has nothing valid to keep.
      const rebuildOffsets = kind === 'lost' || !cooperative

      // Java's onPartitionsLost timing: the partitions are gone before the JoinGroup, not after
      // the sync that replaces them.
      // @see clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.java
      if (kind === 'lost' && previous.length) {
        this.emitPartitions(PARTITIONS_LOST, previous)
      }

      // Java's onJoinPrepare: the same rounds stop advertising ownership before they join, so
      // the JoinGroup carries no `OwnedPartitions` and no peer withholds a partition for a
      // member that has already given it up.
      // @see clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java
      if (rebuildOffsets) {
        this.consumerGroup.forgetOwnedPartitions()
      }

      const roundStartedAt = Date.now()
      const result = await this.consumerGroup.joinAndSync()
      const { revoked, added } = await this.consumerGroup.applyRebalanceResult(result, {
        rebuildOffsets,
      })

      // Java's onJoinComplete: the round decides what the two events carry, not the apply. A
      // cooperative round reports the apply's delta; an eager one reports both whole sets; a lost
      // one reports no revocation at all, because `PARTITIONS_LOST` already named those
      // partitions.
      const assignment = this.consumerGroup.assigned()
      const reportsDelta = cooperative && kind !== 'lost'
      const gaveUp = kind === 'lost' ? [] : reportsDelta ? revoked : previous
      const tookOn = reportsDelta ? added : assignment

      if (gaveUp.length) {
        this.emitPartitions(PARTITIONS_REVOKED, gaveUp)
      }

      if (tookOn.length) {
        this.emitPartitions(PARTITIONS_ASSIGNED, tookOn)
      }

      const payload = {
        groupId: this.groupId,
        memberId: this.consumerGroup.memberId,
        leaderId: this.consumerGroup.leaderId,
        isLeader: this.consumerGroup.isLeader(),
        memberAssignment: assignment.reduce(
          (byTopic, { topic, partitions }) => ({ ...byTopic, [topic]: partitions }),
          {}
        ),
        groupProtocol: this.consumerGroup.groupProtocol,
        membershipProtocol: this.consumerGroup.kind || 'classic',
        duration: Date.now() - roundStartedAt,
        rebalanceProtocol: this.consumerGroup.rebalanceProtocol(),
      }

      this.consumerGroup.ownership?.renew()
      this.onJoinComplete()
      this.instrumentationEmitter.emit(GROUP_JOIN, payload)
      this.logger.info('Consumer has joined the group', payload)

      this.pollTimer.reset()

      // Never re-arm a manager `Runner.stop()` has just cancelled: that would leave heartbeats
      // running after LeaveGroup and a timer surviving stop().
      if (!this.runner.stopRequested) {
        this.heartbeatManager.ensureStarted()
      }

      // `kind` here is the ROUND kind, only 'lost' | 'revoked'. The MEMBERSHIP kind is a separate
      // variable: testing the round kind against 'classic' would be permanently false and would
      // delete the shipped classic cooperative follow-up below. Defaulted, because a group object
      // without the accessor is a classic one.
      const protocol = this.consumerGroup.kind || 'classic'

      kind = this.takePendingKind() || 'revoked'
      reason =
        kind === 'lost'
          ? this.takeRejoinRequested()
          : revoked.length && cooperative && protocol === 'classic'
            ? 'partitions-revoked'
            : protocol === 'consumer' && this.consumerGroup.needsReconciliation()
              ? 'assignment-changed'
              : this.takeRejoinRequested()

      if (reason && protocol === 'consumer' && !this.runner.stopRequested) {
        await this.yieldBetweenRounds()
      }
    } while (reason)
  }

  /**
   * A rebalance this member will never join - today only a fenced static member, which must not
   * rejoin into the same fencing loop. Emits the event a round would have emitted and nothing
   * else: no join, no counter, no chain.
   * @param {string} reason
   * @returns {void}
   */
  abandon(reason) {
    this.instrumentationEmitter.emit(REBALANCING, {
      groupId: this.groupId,
      memberId: this.consumerGroup.memberId,
      membershipProtocol: this.consumerGroup.kind || 'classic',
      reason,
    })
  }

  /**
   * The single entry for every rebalance trigger: the heartbeat timer, the user's `heartbeat()`
   * callback, the fetch path and `recoverFromFetch`.
   *
   * A REVOKED arriving while a chain runs is DROPPED, not queued. The rebalance-in-progress
   * answers a coordinator gives across a round's own sync window are received heartbeats, not
   * verdicts - the Java client ignores `REBALANCE_IN_PROGRESS` unless it is STABLE - and queueing
   * them would chain a round per sync window until the convergence guard crashed the consumer.
   * A LOST is never dropped.
   *
   * @param {Verdict} verdict
   * @returns {void}
   */
  requestRejoin(verdict) {
    if (obsoleteOwnership(this.consumerGroup.ownership, verdict.error)) return
    if (verdict.kind === 'lost') {
      const ownership = this.consumerGroup.ownership
      ownership?.lose(verdict.error, sentOwnership(verdict.error) ?? ownership.token())
    }
    if (this.runner.stopRequested) {
      return
    }

    if (verdict.kind === 'revoked' && this.inFlight) {
      return
    }

    if (this.isStaleGenerationVerdict(verdict)) {
      this.logger.debug('Ignoring a lost-membership answer for a generation already left', {
        groupId: this.groupId,
        memberId: this.consumerGroup.memberId,
        reason: verdict.reason,
        error: verdict.error.message,
      })

      return
    }

    const aborting =
      verdict.kind === 'lost' || this.consumerGroup.rebalanceProtocol() !== COOPERATIVE

    if (aborting) {
      this.verdict = this.mergeVerdict(this.verdict, verdict)
      // Not awaited: `stop()` resolves only once every in-flight handler has returned, and on a
      // poll expiry that handler is by definition overrunning. The sink is required - an
      // un-awaited rejection terminates the process on Node 22+.
      this.fetchManager.stop().catch((error) => {
        this.logger.debug('Error while stopping the fetch manager for a rebalance', {
          error: error.message,
        })
      })
      return
    }

    if (
      this.verdict &&
      (obsoleteOwnership(this.consumerGroup.ownership, this.verdict.error) ||
        this.isStaleGenerationVerdict(this.verdict))
    )
      this.verdict = null

    // Cooperative revocation: nothing stops the fetch manager, so nothing would ever settle
    // `start()` - the round has to be driven from here, in the background.
    this.runRound(verdict.reason, verdict.kind).catch(async (error) => {
      try {
        // `onCrash` owns the CRASH event; if it throws there is nothing left to notify with, and
        // this chain is nobody's `await`, so the rejection would be unobserved.
        await this.runner.onCrash(error)
      } catch (crashHandlerError) {
        this.logger.error('Crash handler failed', {
          groupId: this.groupId,
          memberId: this.consumerGroup.memberId,
          error: crashHandlerError.message || crashHandlerError,
          cause: error.message || error,
        })
      }
    })
  }

  /**
   * Whether a lost-membership verdict is news about a generation this member has already left.
   *
   * Under cooperative rebalancing the delivery path keeps running for the whole round, so a
   * heartbeat or an offset commit issued before the round's JoinGroup answers is still in flight
   * when the coordinator moves the group on. Its answer is `ILLEGAL_GENERATION` or
   * `UNKNOWN_MEMBER_ID` - which, taken at face value, discards this member's uncommitted offsets,
   * stops the world and chains a needless round that reports the partitions it kept as lost.
   *
   * This is the Java client's `generationUnchanged()` rule: a response error only resets the
   * generation when it belongs to the generation the member currently holds. The heartbeat and
   * the offset commit record the generation they were sent under on whatever they throw, which is
   * what makes the comparison possible.
   * @see clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
   *
   * Two verdicts are never stale by this rule: a fenced static member, which must die rather than
   * rejoin, and any verdict that does not come from a group request's answer - a poll-interval
   * overrun, say, which resets the generation itself on purpose and carries no stamp.
   *
   * @param {Verdict} verdict
   * @returns {boolean}
   */
  isStaleGenerationVerdict(verdict) {
    if (verdict.kind !== 'lost' || isFencedInstance(verdict.error)) {
      return false
    }

    // Under the consumer group protocol the member epoch moves on every reconciliation, so
    // comparing the epoch a fencing was sent under against the one held now carries no
    // information - and the epoch is deliberately NOT zeroed when the member is fenced, so every
    // fencing verdict would compare unequal and be dropped, leaving a member that sends nothing
    // and never rejoins.
    if (
      this.consumerGroup.kind === 'consumer' &&
      (isUnknownMemberId(verdict.error) || verdict.error.code === FENCED_MEMBER_EPOCH_CODE)
    ) {
      return false
    }

    const sentUnder = sentGeneration(verdict.error)
    return sentUnder != null && sentUnder !== this.consumerGroup.generationId
  }

  /**
   * Classifies an error into a verdict, or into nothing.
   *
   * A coordinator that must simply be re-discovered is deliberately NOT a verdict: the next
   * round's `join()` re-runs FindCoordinator anyway, which is what the Java client does after
   * `markCoordinatorUnknown`. Raising a rebalance for it would emit a `REBALANCING` for something
   * that is not one.
   *
   * @see clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
   * @param {Error} error
   * @param {string} path one of `REBALANCE_PATHS`
   * @returns {Verdict|null}
   */
  classify(error, path) {
    if (obsoleteOwnership(this.consumerGroup.ownership, error)) return null
    learnOwnershipLoss(this.consumerGroup.ownership, error, path, this.consumerGroup.kind)
    const kind = this.consumerGroup.kind
    const { action } = rebalanceAction(error, path, { kind })

    if (action !== 'lost' && action !== 'revoked') {
      return null
    }

    return { kind: action, reason: rebalanceReason(error, { kind }), error }
  }

  /** @returns {Verdict|null} the slot, cleared */
  takeVerdict() {
    const verdict = this.verdict
    this.verdict = null
    return verdict
  }

  /** Drops whatever is in the slot, without acting on it. @returns {void} */
  clearVerdict() {
    this.verdict = null
  }

  /**
   * The ordered sequence a poll-interval overrun runs. It lives here because step two emits and
   * the heartbeat manager has no emitter.
   *
   * The LeaveGroup precedes the un-awaited fetch-manager stop deliberately: stopping the fetch
   * layer waits for the running handler to return, and on a poll expiry that handler is by
   * definition overrunning. A protocol action must never queue behind it.
   *
   * The leave is awaited, so a rebalance round can start, join and install a new membership while
   * it is in flight. The count of accepted joins is read before the leave for that reason: the leave
   * answers for the membership it was sent for, so once a round has joined there is nothing left to
   * reset and no verdict to raise, and resetting would void the new membership. The count, not the
   * generation, because a rejoin can come back at the same generation or epoch. Otherwise the
   * generation is reset, and a round still in flight owns the rejoin, so no LOST verdict is raised.
   *
   * @param {{ elapsedMs: number, isStatic: boolean }} details
   * @returns {Promise<void>}
   */
  async onPollExpired({ elapsedMs, isStatic }) {
    const maxPollInterval = this.pollTimer.deadline()
    const ownership = this.consumerGroup.ownership
    const expiryError = stampOwnership(
      pollIntervalExceededError({ elapsedMs, maxPollInterval }),
      ownership?.token()
    )
    ownership?.lose(expiryError, ownership.token())

    await this.contain('poll warning', () =>
      this.logger.warn(POLL_TIMEOUT_WARNING, {
        groupId: this.groupId,
        memberId: this.consumerGroup.memberId,
        maxPollInterval,
        elapsedMs,
      })
    )

    await this.contain('poll event', () =>
      this.instrumentationEmitter.emit(POLL_INTERVAL_EXCEEDED, {
        groupId: this.groupId,
        memberId: this.consumerGroup.memberId,
        generationId: this.consumerGroup.generationId,
        maxPollInterval,
        elapsedMs,
        isStatic,
      })
    )

    if (this.runner.stopRequested) {
      return
    }

    await this.contain('heartbeat stop', () => this.heartbeatManager.stop())

    const joinsBefore = this.consumerGroup.joinCount

    // KIP-848 members always tell the coordinator they are gone, static or not - the protocol picks
    // epoch -1 or -2 from `groupInstanceId`. KIP-345's "a static member sends nothing" rule is the
    // CLASSIC rule, and staying silent here would leave a static consumer-protocol member out of
    // `STALE`, so it would never rejoin.
    // @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
    if (this.consumerGroup.kind === 'consumer') {
      await this.contain('poll leave', () => this.consumerGroup.leaveForPollExpiry())
    } else if (!isStatic) {
      await this.contain('poll leave', () => this.consumerGroup.leave())
    }

    if (this.consumerGroup.joinCount !== joinsBefore) {
      return
    }

    await this.contain('generation reset', () => this.consumerGroup.resetGenerationLocally())

    // A round that started during the leave above already rejoins this member. A second LOST would
    // stay in the slot after that round ends, and an orphan there reads as a rebalance still owed.
    if (this.isRebalancing()) {
      return
    }

    await this.contain('rejoin', () =>
      this.requestRejoin({
        kind: 'lost',
        reason: 'poll-interval-exceeded',
        error: expiryError,
      })
    )
  }

  /**
   * Runs one cleanup step without letting it suppress subsequent steps, even if logging fails.
   * @param {string} label
   * @param {() => unknown} work
   * @returns {Promise<unknown>} undefined after failure
   */
  async contain(label, work) {
    try {
      return await work()
    } catch (error) {
      try {
        this.logger.error(`Poll expiry ${label} failed`, {
          error: describeThrown(error, 'message'),
        })
      } catch {}
    }
  }

  /** @returns {Promise<void>} resolves when no chain is running */
  idle() {
    return this.inFlight || Promise.resolve()
  }

  /**
   * LOST beats REVOKED. The two differ on whether uncommitted offsets may be committed, so a
   * REVOKED must never overwrite a LOST. Two verdicts of the same kind collapse to the first.
   *
   * A pending verdict that is already stale is replaced, whatever its kind. It would only be dropped
   * when acted on, and keeping it would drop the fresh verdict with it: a heartbeat failure from
   * the generation before a poll-interval overrun parks a LOST first, the overrun then resets the
   * generation, and the overrun's own LOST - the only one that still has to run a round - would
   * collapse into the stale one. A stale LOST still says the generation the offsets were processed
   * under is gone, so a REVOKED replacing it is turned into a LOST that keeps the REVOKED's reason
   * and error: the round still runs, and the offsets are discarded instead of committed.
   *
   * Public, not private: the `Runner`'s retrier calls it to merge a thrown error with whatever
   * is already in the slot, which is what keeps one rebalance to one round.
   * @param {Verdict|null} pending
   * @param {Verdict} incoming
   * @returns {Verdict}
   */
  mergeVerdict(pending, incoming) {
    if (!pending) {
      return incoming
    }

    if (this.isStaleGenerationVerdict(pending)) {
      return incoming.kind === 'revoked' ? { ...incoming, kind: 'lost' } : incoming
    }

    if (pending.kind === 'lost') {
      return pending
    }

    return incoming.kind === 'lost' ? incoming : pending
  }

  /**
   * Keeps the first pending reason: a second and third trigger during one chain still produce one
   * follow-up round. A different helper from `mergeVerdict` - this one merges reasons, not
   * precedence.
   * @private
   * @param {string|null} pending
   * @param {string|null} incoming
   * @returns {string|null}
   */
  mergeReason(pending, incoming) {
    return pending || incoming
  }

  /** @private @returns {string|null} the pending reason, cleared */
  takeRejoinRequested() {
    const reason = this.rejoinRequested
    this.rejoinRequested = null
    return reason
  }

  /** @private @returns {'lost'|null} the pending kind, cleared */
  takePendingKind() {
    const kind = this.pendingKind
    const stale = kind === 'lost' && this.pendingJoinCount < this.consumerGroup.joinCount
    this.pendingKind = null
    this.pendingJoinCount = null
    if (stale) {
      this.rejoinRequested = null
      return null
    }
    return kind
  }

  /**
   * Emits one of the three partition events with the payload every listener expects.
   *
   * Not private: the group membership emits `PARTITIONS_LOST` through this method for a loss it
   * learns about outside a round, so the coordinator stays the only emitter of these three events.
   * @param {string} event one of the three partition event names
   * @param {import('./groupProtocol/interface').TopicPartitions[]} partitions
   * @returns {void}
   */
  emitPartitions(event, partitions) {
    this.instrumentationEmitter.emit(event, {
      groupId: this.groupId,
      memberId: this.consumerGroup.memberId,
      generationId: this.consumerGroup.generationId,
      // A copy: the arrays behind this are the member's own assignment state, and a listener that
      // sorts or splices the payload must not edit it.
      partitions: partitions.map(({ topic, partitions }) => ({
        topic,
        partitions: [...partitions],
      })),
    })
  }
}
