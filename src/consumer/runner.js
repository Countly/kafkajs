const { EventEmitter } = require('events')
const Long = require('../utils/long')
const createRetry = require('../retry')
const { isKafkaJSError, isFencedInstance, isUnknownMemberId } = require('../errors')

const {
  events: { FETCH, FETCH_START, START_BATCH_PROCESS, END_BATCH_PROCESS },
} = require('./instrumentationEvents')
const createFetchManager = require('./fetchManager')
const RebalanceCoordinator = require('./rebalanceCoordinator')
const PollTimer = require('./pollTimer')
const HeartbeatManager = require('./heartbeatManager')
const { REBALANCE_PATHS, rebalanceReason } = require('./rebalanceErrorContract')
const { COOPERATIVE } = require('./groupProtocol/interface')

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
   * @param {(reason: Error) => void} options.onCrash
   * @param {import("../../types").RetryOptions} [options.retry]
   * @param {boolean} [options.autoCommit=true]
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
    onCrash,
    retry,
    autoCommit = true,
  }) {
    super()
    this.logger = logger.namespace('Runner')
    this.consumerGroup = consumerGroup
    this.instrumentationEmitter = instrumentationEmitter
    this.eachBatchAutoResolve = eachBatchAutoResolve
    this.eachBatch = eachBatch
    this.eachMessage = eachMessage
    this.heartbeatInterval = heartbeatInterval
    this.retrier = createRetry(Object.assign({}, retry))
    this.onCrash = onCrash
    this.autoCommit = autoCommit
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

    if (verdict) {
      this.rebalanceCoordinator.requestRejoin(verdict)
    }
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
        // ever cancel it, and the handle is not unref'd. Last word deliberately: it runs after
        // `onCrash` (and the disconnect/stop inside it), so a late re-arm is undone too.
        this.heartbeatManager.stop()
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

    this.retrier(async (bail, retryCount, retryTime) => {
      if (!this.running) {
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
          this.heartbeatManager.stop()
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
    const lost = verdict.kind === 'lost'
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

      return await this.resolveRebalanceVerdict(verdict)
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

    return classified || { kind: null, reason: rebalanceReason(input), error: input }
  }

  /**
   * The verdict half of `handleRebalanceError`: discard or auto-commit, then rejoin.
   * @private
   * @param {{ kind: string|null, reason: string|null, error: Error }} verdict
   * @returns {Promise<void>}
   * @throws {Error} the verdict's error for a fenced member, and for anything that is not a
   *   rebalance
   */
  async resolveRebalanceVerdict({ kind, reason, error: e }) {
    if (kind === 'lost') {
      this.logger.error('Membership lost, discarding uncommitted offsets and re-joining', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        error: e.message,
      })

      this.consumerGroup.discardUncommittedOffsets()

      if (isFencedInstance(e)) {
        this.rebalanceCoordinator.abandon('lost')
        throw e
      }

      if (isUnknownMemberId(e)) {
        // Only an unrecognized member id needs a fresh one. A stale generation
        // (ILLEGAL_GENERATION) does not invalidate the member id itself, so keep it and avoid an
        // extra MEMBER_ID_REQUIRED round-trip.
        this.consumerGroup.memberId = null
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

      await this.autoCommitOffsets()
      await this.rebalanceCoordinator.runRound(reason, 'revoked')
      return
    }

    throw e
  }

  /**
   * Stop the runner: halt fetching, wait for in-flight processing, then leave the group.
   *
   * A static member (`groupInstanceId` set) keeps its group slot across a normal shutdown
   * instead (KIP-345 default: Java gates this the same way via
   * `AbstractCoordinator.shouldSendLeaveGroupRequest = LEAVE_GROUP || isDynamicMember()`), so
   * no LeaveGroup is sent for it. There is no override for this today (KIP-1092 adds an
   * explicit one in a later phase) — lower `sessionTimeout`, or drop `groupInstanceId`, to bound
   * or remove the delay before the group reassigns a static member's partitions.
   *
   * @returns {Promise<void>}
   */
  async stop() {
    if (!this.running) {
      return
    }

    this.logger.debug('stop consumer group', {
      groupId: this.consumerGroup.groupId,
      memberId: this.consumerGroup.memberId,
    })

    this.stopRequested = true
    this.running = false
    this.heartbeatManager.stop()
    this.pollTimer.stop()
    this.rebalanceCoordinator.clearVerdict()

    try {
      // A background cooperative round must finish before LeaveGroup, or its JoinGroup would
      // re-register a member this runner has just left. Bounded by the current round, which is
      // the property waitForConsumer() already has.
      await this.rebalanceCoordinator.idle()
      await this.fetchManager.stop()
      await this.waitForConsumer()

      this.consumerGroup.clearAssignerState()

      if (this.consumerGroup.groupInstanceId) {
        this.consumerGroup.memberId = null
      } else {
        await this.consumerGroup.leave()
      }
    } catch (e) {}
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
   * It classifies and routes, and does nothing else: the offset work belongs to
   * `handleRebalanceError`, which is reached once per rebalance. Under cooperative rebalancing a
   * revocation RESOLVES instead of throwing, so the error never reaches the worker, the fetcher
   * or the fetch manager's own teardown - which is the whole point of not stopping the world.
   * @returns {Promise<void>}
   */
  async heartbeat() {
    try {
      await this.consumerGroup.heartbeat({ interval: this.heartbeatInterval })
    } catch (e) {
      const verdict = this.rebalanceCoordinator.classify(e, REBALANCE_PATHS.HEARTBEAT)

      if (verdict) {
        this.rebalanceCoordinator.requestRejoin(verdict)
      }

      if (
        verdict &&
        verdict.kind === 'revoked' &&
        this.consumerGroup.rebalanceProtocol() === COOPERATIVE
      ) {
        return
      }

      throw e
    }
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
        await this.autoCommitOffsets()
        throw e
      }

      this.consumerGroup.resolveOffset({
        topic,
        partition,
        offset: message.offset,
        leaderEpoch: message.batchContext ? message.batchContext.partitionLeaderEpoch : -1,
      })
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
            return this.commitOwnedOffsets(offsets)
          }

          // The no-argument form commits on this batch's behalf, so it follows this batch's
          // partition: a handler still running for one a rebalance revoked must not move an
          // offset the new owner controls.
          if (!this.consumerGroup.isAssigned(topic, partition)) {
            this.logger.debug('Skipping a commit for a partition this member no longer holds', {
              groupId: this.consumerGroup.groupId,
              memberId: this.consumerGroup.memberId,
              topic,
              partition,
            })

            return
          }

          return this.consumerGroup.commitOffsetsIfNecessary()
        },
        uncommittedOffsets: () => this.consumerGroup.uncommittedOffsets(),
        isRunning: () => this.running,
        isStale: () => this.consumerGroup.hasSeekOffset({ topic, partition }),
      })
    } catch (e) {
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
      await this.autoCommitOffsets()
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
   * @param {import('../../types').OffsetsByTopicPartition} offsets
   * @returns {Promise<void>}
   */
  async commitOwnedOffsets(offsets) {
    if (!Array.isArray(offsets.topics)) {
      return this.consumerGroup.commitOffsets(offsets)
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

    return this.consumerGroup.commitOffsets({ ...offsets, topics })
  }

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

    const batches = await this.consumerGroup.fetch(nodeId)

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
    }

    return batches
  }

  /**
   * Whether a batch has outlived the assignment it was fetched under.
   *
   * Two ways it can: the partition moved to another member, or it stayed but the offsets were
   * replaced and the new assignment is already re-fetching the same records. Either way,
   * delivering it duplicates records and committing for it moves an offset this member no longer
   * controls. Checked when a batch is picked up AND between messages, because a rebalance can
   * land in the middle of a batch a handler is already working through.
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

    if (
      batch.assignmentEpoch != null &&
      batch.assignmentEpoch !== this.consumerGroup.assignmentEpoch
    ) {
      return true
    }

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
        await this.autoCommitOffsetsIfNecessary()

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

      if (this.eachMessage) {
        await this.processEachMessage(batch)
      } else if (this.eachBatch) {
        await this.processEachBatch(batch)
      }

      this.instrumentationEmitter.emit(END_BATCH_PROCESS, {
        ...payload,
        duration: Date.now() - startBatchProcess,
      })

      await this.autoCommitOffsets()
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

  autoCommitOffsets() {
    if (this.autoCommit) {
      return this.consumerGroup.commitOffsets()
    }
  }

  autoCommitOffsetsIfNecessary() {
    if (this.autoCommit) {
      return this.consumerGroup.commitOffsetsIfNecessary()
    }
  }

  commitOffsets(offsets) {
    if (!this.running) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        offsets,
      })
      return
    }

    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        await this.consumerGroup.commitOffsets(offsets)
      } catch (e) {
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
    })
  }
}
