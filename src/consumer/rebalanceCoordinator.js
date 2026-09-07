const { KafkaJSNonRetriableError, isFencedInstance } = require('../errors')
const {
  rebalanceAction,
  rebalanceReason,
  pollIntervalExceededError,
  sentGeneration,
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
  }) {
    this.runner = runner
    this.consumerGroup = consumerGroup
    this.fetchManager = fetchManager
    this.heartbeatManager = heartbeatManager
    this.pollTimer = pollTimer
    this.instrumentationEmitter = instrumentationEmitter
    this.logger = logger.namespace('RebalanceCoordinator')
    this.groupId = groupId

    /** @type {Promise<void>|null} the running chain; also the single-flight marker */
    this.inFlight = null
    /** @type {string|null} a reason a trigger left behind while a chain was running */
    this.rejoinRequested = null
    /** @type {'lost'|null} set when the trigger left behind was a LOST */
    this.pendingKind = null
    /** @type {Verdict|null} the aborting-path slot */
    this.verdict = null
    /** Consecutive rounds in the current chain. */
    this.rounds = 0
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
      // A chain that threw must not leave stale-assignment detection off for the life of the
      // group: the latch is cleared here as well as by a successful apply.
      this.consumerGroup.clearStaleAssignmentPending()
    })

    return this.inFlight
  }

  /**
   * The round loop itself: join, sync, apply, report, and decide whether another round is owed.
   * @private
   * @param {string|null} reason
   * @param {'lost'|'revoked'|'initial'} kind
   * @returns {Promise<void>}
   * @throws {KafkaJSNonRetriableError} when the chain does not converge
   */
  async runChain(reason, kind) {
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
          reason,
        })
      }

      const previous = this.consumerGroup.assigned()

      // Java's onPartitionsLost timing: the partitions are gone before the JoinGroup, not after
      // the sync that replaces them.
      // @see clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.java
      if (kind === 'lost' && previous.length) {
        this.emitPartitions(PARTITIONS_LOST, previous)
      }

      const roundStartedAt = Date.now()
      const result = await this.consumerGroup.joinAndSync()
      // Only the delta is read here: the GROUP_JOIN payload below is built from the APPLIED
      // assignment through `consumerGroup.assigned()`, which is what keeps it the full set.
      // A cooperative round runs alongside the fetch loop, so the apply must keep what this
      // member retained; an eager round revoked everything before joining and rebuilds.
      const { revoked, added } = await this.consumerGroup.applyRebalanceResult(result, {
        rebuildOffsets: this.consumerGroup.rebalanceProtocol() !== COOPERATIVE,
      })

      if (revoked.length) {
        this.emitPartitions(PARTITIONS_REVOKED, revoked)
      }

      if (added.length) {
        this.emitPartitions(PARTITIONS_ASSIGNED, added)
      }

      const payload = {
        groupId: this.groupId,
        memberId: this.consumerGroup.memberId,
        leaderId: this.consumerGroup.leaderId,
        isLeader: this.consumerGroup.isLeader(),
        memberAssignment: this.consumerGroup
          .assigned()
          .reduce((byTopic, { topic, partitions }) => ({ ...byTopic, [topic]: partitions }), {}),
        groupProtocol: this.consumerGroup.groupProtocol,
        duration: Date.now() - roundStartedAt,
        rebalanceProtocol: this.consumerGroup.rebalanceProtocol(),
      }

      this.instrumentationEmitter.emit(GROUP_JOIN, payload)
      this.logger.info('Consumer has joined the group', payload)

      this.pollTimer.reset()

      // Never re-arm a manager `Runner.stop()` has just cancelled: that would leave heartbeats
      // running after LeaveGroup and a timer surviving stop().
      if (!this.runner.stopRequested) {
        this.heartbeatManager.ensureStarted()
      }

      const cooperative = this.consumerGroup.rebalanceProtocol() === COOPERATIVE

      kind = this.takePendingKind() || 'revoked'
      reason =
        kind === 'lost'
          ? this.takeRejoinRequested()
          : revoked.length && cooperative
            ? 'partitions-revoked'
            : this.takeRejoinRequested()
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

    const sentUnder = sentGeneration(verdict.error)
    return sentUnder != null && sentUnder !== this.consumerGroup.generationId
  }

  /**
   * Classifies an error into a verdict, or into nothing.
   *
   * A coordinator that must simply be re-discovered is deliberately NOT a verdict: the cached
   * coordinator handle is dropped and the next attempt re-runs FindCoordinator, which is what the
   * Java client does with `markCoordinatorUnknown`. Raising a rebalance for it would emit a
   * `REBALANCING` for something that is not one.
   *
   * @see clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
   * @param {Error} error
   * @param {string} path one of `REBALANCE_PATHS`
   * @returns {Verdict|null}
   */
  classify(error, path) {
    const { action } = rebalanceAction(error, path)

    if (action !== 'lost' && action !== 'revoked') {
      return null
    }

    return { kind: action, reason: rebalanceReason(error), error }
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
   * @param {{ elapsedMs: number, isStatic: boolean }} details
   * @returns {Promise<void>}
   */
  async onPollExpired({ elapsedMs, isStatic }) {
    const maxPollInterval = this.pollTimer.deadline()

    this.logger.warn(POLL_TIMEOUT_WARNING, {
      groupId: this.groupId,
      memberId: this.consumerGroup.memberId,
      maxPollInterval,
      elapsedMs,
    })

    this.instrumentationEmitter.emit(POLL_INTERVAL_EXCEEDED, {
      groupId: this.groupId,
      memberId: this.consumerGroup.memberId,
      generationId: this.consumerGroup.generationId,
      maxPollInterval,
      elapsedMs,
      isStatic,
    })

    if (this.runner.stopRequested) {
      return
    }

    this.heartbeatManager.stop()

    if (!isStatic) {
      await this.consumerGroup.leave().catch((error) => {
        this.logger.debug('LeaveGroup failed after a poll-interval overrun', {
          error: error.message,
        })
      })
    }

    this.consumerGroup.resetGenerationLocally()

    this.requestRejoin({
      kind: 'lost',
      reason: 'poll-interval-exceeded',
      error: pollIntervalExceededError({ elapsedMs, maxPollInterval }),
    })
  }

  /** @returns {Promise<void>} resolves when no chain is running */
  idle() {
    return this.inFlight || Promise.resolve()
  }

  /**
   * LOST beats REVOKED. The two differ on whether uncommitted offsets may be committed, so a
   * REVOKED must never overwrite a LOST. Two verdicts of the same kind collapse to the first.
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
    this.pendingKind = null
    return kind
  }

  /**
   * @private
   * @param {string} event one of the three partition event names
   * @param {import('./groupProtocol/interface').TopicPartitions[]} partitions
   * @returns {void}
   */
  emitPartitions(event, partitions) {
    this.instrumentationEmitter.emit(event, {
      groupId: this.groupId,
      memberId: this.consumerGroup.memberId,
      generationId: this.consumerGroup.generationId,
      partitions,
    })
  }
}
