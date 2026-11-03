const createRetry = require('../../retry')
const {
  KafkaJSDeliveryTimeout,
  KafkaJSInvariantViolation,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const { RETRIABLE_CATEGORIES } = require('../../protocol/errorCategories')
const { classify, transactionStateFor } = require('./errorClassifier')
const ProducerIdAndEpoch = require('./producerIdAndEpoch')
const PartitionSequencer = require('./partitionSequencer')
const TransactionState = require('./transactionState')
const TransactionVersion = require('./transactionVersion')
const TransactionOffsets = require('./transactionOffsets')
const {
  READY,
  IN_TRANSACTION,
  COMMITTING,
  FATAL_ERROR,
  isArmed,
} = require('../eosManager/transactionStates')

/** @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java */
const OUT_OF_ORDER_SEQUENCE_NUMBER = 45
const INVALID_PRODUCER_ID_MAPPING = 49
const UNKNOWN_PRODUCER_ID = 59

/**
 * Java's message for a transaction whose expired batches left a gap the producer cannot close.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
 */
const UNACKED_MESSAGES =
  "The client hasn't received acknowledgment for some previously sent messages and can no " +
  'longer retry them. It is safe to abort the transaction and continue.'

/** Returned by `ensureProducerId()` while a producer id is held; never rejects. */
const HELD = Promise.resolve()

const ignore = () => {}

/**
 * @typedef {import('./partitionSequencer').PartitionBatch} PartitionBatch
 * @typedef {import('./partitionSequencer').LeaderMap} LeaderMap
 * @typedef {import('./partitionSequencer').Verdict} Verdict
 * @typedef {import('../eosManager/transactionStates').State} State
 * @typedef {import('../../protocol/errorCategories').Category} Category
 * @typedef {import('../../protocol/errorCategories').Surface} Surface
 */

/**
 * The producer's state: its producer id and epoch, its per-partition sequences, its transaction
 * state and transaction version, and the classification of every error that reaches them. It is
 * the only thing that changes the producer's identity, and it turns each failed batch's verdict
 * into the consequence the Java producer applies: an epoch bump, a sequence restart or rebase, or
 * an armed error state.
 *
 * One instance per `eosManager`: the idempotent producer's and, separately, the transactional
 * one's. It sends no Produce request itself; the send path drains it, reports every batch's
 * outcome to `complete()` or `fail()` exactly once, and settles the batch's callers.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
 */
class ProducerStateManager {
  #logger
  #transactional
  #retries
  #retry
  #retrier
  #getGeneration
  #addPartitionsToTxn
  #identity
  /** @type {Map<string, Set<number>>} partitions in the current transaction */
  #partitions = new Map()
  /** @type {Map<string, Map<number, Promise<void>>>} partitions whose AddPartitionsToTxn is pending */
  #adding = new Map()
  /** @type {Map<string, number[]> | null} the partitions the next AddPartitionsToTxn carries */
  #nextAdd = null
  #nextAddPromise = null
  /** @type {Set<Promise<void>>} every AddPartitionsToTxn not yet settled, never rejecting */
  #adds = new Set()
  #started = false
  /** Moves on every return to READY, so a late answer cannot touch the next transaction. */
  #generation = 0
  /** @type {?{ lifecycle: ?import('../generation'), promise: Promise<void> }} */
  #acquisition = null
  /** @type {?{ lifecycle: ?import('../generation'), promise: Promise<void> }} */
  #recovery = null
  #sequencer
  #transaction
  #version
  #offsets

  /**
   * @param {object} options
   * @param {import('../../../types').Logger} options.logger
   * @param {boolean} [options.transactional=false]
   * @param {string|null} [options.transactionalId=null] sent on InitProducerId by a
   *   transactional manager only; a manager given one, whether or not it is transactional, belongs
   *   to a transactional producer, which changes the advice a refused InitProducerId carries
   * @param {number} options.transactionTimeout
   * @param {import('../../../types').RetryOptions} [options.retry={}] the producer's retry
   *   options for the requests this class makes on the send path - AddPartitionsToTxn, and a
   *   producer id lost to the epoch ceiling
   * @param {number} [options.deliveryRetries=retry.retries] how many times a batch is sent again
   * @param {(lifecycle?: import('../generation')) => Promise<import('../../../types').Broker>} options.findBroker
   *   the broker an InitProducerId goes to, asked again for every request with the producer
   *   lifecycle the operation captured
   * @param {(topics: { topic: string, partitions: number[] }[], lifecycle?: import('../generation')) => Promise<void>} options.addPartitionsToTxn
   *   sends one AddPartitionsToTxn with the current producer id and epoch
   * @param {() => (import('../generation')|undefined)} [options.getGeneration] the producer's
   *   current lifecycle. Each operation reads it once when it starts - never after an await - and
   *   its retries stop, and its answer changes nothing, once that lifecycle is closed.
   */
  constructor({
    logger,
    transactional = false,
    transactionalId = null,
    transactionTimeout,
    retry = {},
    deliveryRetries = retry.retries,
    findBroker,
    addPartitionsToTxn,
    getGeneration = () => undefined,
  }) {
    this.#logger = logger
    this.#transactional = transactional
    this.#retries = deliveryRetries ?? createRetry.defaults.retries
    this.#retry = retry
    this.#retrier = createRetry(retry)
    this.#getGeneration = getGeneration
    this.#addPartitionsToTxn = addPartitionsToTxn
    this.#sequencer = new PartitionSequencer({ logger })
    this.#transaction = new TransactionState()
    this.#version = new TransactionVersion()
    this.#offsets = new TransactionOffsets(logger)
    this.#identity = new ProducerIdAndEpoch({
      logger,
      transactionalId: transactional ? transactionalId : null,
      transactionalProducer: Boolean(transactionalId),
      transactionTimeout,
      findBroker,
      onReset: () => this.#resetSequences(),
      getGeneration,
    })
  }

  /** @returns {PartitionSequencer} the per-partition sequencing, read-only */
  get sequencer() {
    return this.#sequencer
  }

  /** @returns {TransactionState} the transaction state machine, read-only */
  get transaction() {
    return this.#transaction
  }

  /** @returns {TransactionVersion} the transaction version reader, read-only */
  get version() {
    return this.#version
  }

  /** @returns {TransactionOffsets} offsets owned by this transaction, never the send path */
  get offsets() {
    return this.#offsets
  }

  /** @returns {string|number} the producer id, -1 while none is held */
  get producerId() {
    return this.#identity.producerId
  }

  /** @returns {number} the producer epoch, -1 while no producer id is held */
  get producerEpoch() {
    return this.#identity.producerEpoch
  }

  /** @returns {boolean} whether a producer id is held */
  isInitialized() {
    return this.#identity.isValid()
  }

  /** @returns {State} */
  state() {
    return this.transaction.state()
  }

  /** @returns {1|2} the transaction version in force for the current transaction */
  transactionVersion() {
    return this.version.current()
  }

  /**
   * Classifies an error for the request it arrived on and stamps its category in place.
   * @param {Error} error
   * @param {Surface} surface
   * @returns {{ category: Category|null, duplicate?: true }}
   */
  classify(error, surface) {
    return classify(error, surface, { transactional: this.#transactional })
  }

  /**
   * Acquires a producer id: one InitProducerId, shared with any acquisition already running. A
   * new producer id restarts every partition's sequences; while batches are still in flight -
   * only after the epoch ceiling lost the previous producer id - every partition is marked stale
   * instead and restarts once its own batches have settled, as after any local bump. A partition
   * whose batches in flight were all restarted since they were sent is left alone: its re-sends
   * already go out from 0 under the new producer id.
   * @returns {Promise<void>}
   * @throws {import('../../errors').KafkaJSInvalidConfigurationError} CLUSTER_AUTHORIZATION_FAILED
   * @throws {Error} any other error of the request, unchanged; nothing is armed
   * @throws {Error} the close error of the producer lifecycle, when it closed meanwhile
   */
  initialize() {
    const lifecycle = this.#getGeneration()
    const pending = this.#identity.pendingInitialization()
    if (pending !== null) {
      return pending
    }
    if (this.#acquisition !== null && this.#acquisition.lifecycle === lifecycle) {
      return this.#acquisition.promise
    }

    const acquisition = { lifecycle, promise: null }
    acquisition.promise = this.#identity.initialize().finally(() => {
      if (this.#acquisition === acquisition) this.#acquisition = null
    })
    this.#acquisition = acquisition
    return acquisition.promise
  }

  /**
   * Bumps the epoch of the producer id held (KIP-360), at a transaction boundary.
   * @returns {Promise<void>}
   * @throws {KafkaJSInvariantViolation} while a batch is in flight
   * @throws {import('../../errors').KafkaJSApplicationRecoverableError} when the coordinator did
   *   not bump; the producer is then in `FATAL_ERROR`, as after any fatal error of the request
   * @throws {Error} a retriable error, unchanged, for the caller to retry
   */
  async reinitialize() {
    if (this.sequencer.hasInFlight()) {
      throw new KafkaJSInvariantViolation('Producer id re-initialized with a batch in flight')
    }

    try {
      await this.#identity.reinitialize()
    } catch (error) {
      this.classify(error, 'txn')
      if (transactionStateFor(error) === FATAL_ERROR) {
        this.arm(error)
      }
      throw error
    }
  }

  /**
   * Makes sure a producer id is held before the send path drains: when the epoch ceiling's own
   * InitProducerId failed and nothing is pending, acquires one again, retrying retriable errors
   * with the producer's retry options. A fatal failure arms `FATAL_ERROR`; any other rejects the
   * queued batches with it and leaves the next call to try again, so no send waits for a producer
   * id that cannot come. Shared between concurrent callers.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
   * @returns {Promise<void>} never rejects
   */
  ensureProducerId() {
    if (this.#identity.isValid()) {
      return HELD
    }

    const lifecycle = this.#getGeneration()
    if (this.#recovery === null || this.#recovery.lifecycle !== lifecycle) {
      const recovery = { lifecycle, promise: null }
      recovery.promise = this.#recover(lifecycle).finally(() => {
        if (this.#recovery === recovery) this.#recovery = null
      })
      this.#recovery = recovery
    }
    return this.#recovery.promise
  }

  /**
   * Takes the producer id and epoch an EndTxn v5 response carries (KIP-890) and restarts every
   * partition's sequences.
   * @param {{ producerId: string|number, producerEpoch: number }} pair
   * @returns {void}
   * @throws {KafkaJSInvariantViolation} while a batch is in flight, changing nothing
   */
  adopt({ producerId, producerEpoch }) {
    if (this.sequencer.hasInFlight()) {
      throw new KafkaJSInvariantViolation('Producer id and epoch adopted with a batch in flight')
    }

    this.#identity.adopt({ producerId, producerEpoch })
  }

  /**
   * The one way an error state is armed. An error whose state is `FATAL_ERROR` arms it, for
   * either producer kind; any other arms `ABORTABLE_ERROR` on a transactional producer inside a
   * transaction and does nothing otherwise. Every batch still queued is then rejected with the
   * armed error - the same object for all - and nothing queued later is drained. An error that
   * leaves the state as it was, a second fatal one included, is neither logged nor acted on.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
   * @param {Error} error a classified error, or one of the category classes
   * @returns {void}
   */
  arm(error) {
    const state = this.transaction.state()
    if (transactionStateFor(error) === FATAL_ERROR) {
      this.transaction.armFatal(error)
    } else if (this.#transactional && (state === IN_TRANSACTION || state === COMMITTING)) {
      this.transaction.armAbortable(error)
    }

    if (this.transaction.state() === state) {
      return
    }

    if (this.transaction.state() === FATAL_ERROR) this.offsets.release()
    const armed = this.transaction.armedError()
    this.#logger.error(`Transaction state ${state} --> ${this.transaction.state()}`, {
      error: armed.message,
    })
    this.sequencer.abortQueued(armed)
  }

  /**
   * Moves the transaction state along one of its edges and logs it; entering `READY` forgets the
   * partitions, offsets and pending AddPartitionsToTxn of the transaction that ended, so the next
   * transaction sends its own request for a partition the old one was still adding.
   * @param {State} state
   * @returns {void}
   * @throws {import('../../errors').KafkaJSNonRetriableError} for an edge the state machine does
   *   not have
   */
  transitionTo(state) {
    const from = this.transaction.state()
    this.transaction.transitionTo(state)
    this.#logger.debug(`Transaction state transition ${from} --> ${state}`)
    if (state === READY) {
      this.offsets.release()
      this.#adding.clear()
      this.#nextAdd = null
      this.#partitions.clear()
      this.#started = false
      this.#generation += 1
    }
  }

  /**
   * Queues batches for the drain. While an error state is armed they are rejected at once with
   * the armed error.
   * @param {PartitionBatch[]} batches
   * @returns {void}
   */
  enqueue(batches) {
    this.sequencer.enqueue(batches)
    if (this.#armed()) {
      this.sequencer.abortQueued(this.transaction.armedError())
    }
  }

  /**
   * The batches that may go to `broker` now, numbered and stamped. Settles any unresolved
   * sequence first, and drains nothing while an error state is armed or no producer id is held.
   * While armed it also rejects every batch still queued with the armed error, a re-send the
   * caller requeued after the response that armed included; Java's sender likewise aborts
   * undrained batches on every pass while an error is armed.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
   * @param {{ nodeId: number|string }} broker
   * @param {LeaderMap} leaders
   * @param {import('./recordSize').RequestBudget} [budget] the request-size plan the drain keeps
   *   one request within
   * @returns {PartitionBatch[]}
   */
  drain(broker, leaders, budget) {
    this.resolveSequences()
    if (this.#armed()) {
      this.sequencer.abortQueued(this.transaction.armedError())
      return []
    }

    return this.sequencer.drain(broker.nodeId, leaders, {
      now: Date.now(),
      producerIdValid: this.#identity.isValid(),
      producerId: this.#identity.producerId,
      producerEpoch: this.#identity.producerEpoch,
      isPartitionInTransaction: this.#isPartitionInTransaction,
      budget,
    })
  }

  /**
   * Settles the unresolved sequences of partitions with nothing left in flight: a gap bumps the
   * epoch of an idempotent-only producer and arms `ABORTABLE_ERROR` on a transactional one, which
   * rejects the queued batches. One pass makes at most one bump, since the bump is producer-wide.
   * A stale partition's gap needs no new identity: the earlier bump already moved it, and the next
   * drain restarts it at sequence 0 (the sequencer reports it `'cleared'`, which does not
   * acknowledge the expired send). Runs before every drain, and before a transaction's `EndTxn`, as
   * the Java sender resolves sequences on every pass, so no transaction commits over a batch the
   * broker may or may not hold.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @returns {void}
   */
  resolveSequences() {
    let bumped = false
    for (const { topic, partition, outcome } of this.sequencer.resolveUnresolved()) {
      if (outcome === 'cleared') {
        continue
      }

      if (this.#transactional) {
        this.arm(new KafkaJSTransactionAbortableError(UNACKED_MESSAGES))
      } else if (!bumped) {
        this.#bump(topic, partition)
        bumped = true
      }
    }
  }

  /**
   * Settles a batch the broker acknowledged, or one the broker already had
   * (`{ duplicate: true }`).
   * @param {PartitionBatch} batch
   * @param {{ baseOffset: string|number, logStartOffset?: string|number } | { duplicate: true }} result
   * @returns {void}
   */
  complete(batch, result) {
    this.sequencer.complete(batch, result)
  }

  /**
   * Decides what happens to a batch whose Produce failed, applies the producer-id, sequence and
   * state consequence, and returns the verdict for the caller to act on:
   *
   * - `COMPLETE_AS_DUPLICATE`: nothing applied; the caller calls `complete(batch, { duplicate: true })`.
   * - `RETRY_IN_ORDER`, `BUMP_EPOCH`: the epoch is bumped (`BUMP_EPOCH`) or a transactional
   *   partition whose log was truncated restarts at 0; the caller re-queues the batch through
   *   `sequencer.requeue()` with its backoff. While an error state is armed the batch is failed
   *   instead and the verdict is `FAIL_BATCH`: nothing is drained once armed.
   * - `FAIL_BATCH`, `FAIL_BATCH_AND_REBASE`, `FAIL_BATCH_AND_BUMP`: the batch has left the
   *   sequencer - with its sequences given back, or the epoch bumped - and the caller rejects it.
   *   `FAIL_BATCH` arms `ABORTABLE_ERROR` for a transactional `UNKNOWN_PRODUCER_ID` or
   *   `OUT_OF_ORDER_SEQUENCE_NUMBER` and for an expired batch inside a transaction, and
   *   `FATAL_ERROR` for `INVALID_PRODUCER_ID_MAPPING`; an expired batch, and a re-sent one
   *   that ran out of retries on a retriable error, leave their partition's sequence unresolved -
   *   the broker may hold them - and a transactional `UNKNOWN_PRODUCER_ID`
   *   resets its partition (an idempotent-only one never ends in `FAIL_BATCH` for it).
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @param {PartitionBatch} batch a batch that was drained and not yet settled
   * @param {Error} error the partition's error, or a `KafkaJSDeliveryTimeout` for an expired batch
   * @param {{ logStartOffset?: string|number, transmitted?: boolean }} [partitionResponse={}] the partition's Produce
   *   response, for `UNKNOWN_PRODUCER_ID`
   * @returns {Verdict}
   */
  fail(batch, error, { logStartOffset, transmitted } = {}) {
    const expired = error instanceof KafkaJSDeliveryTimeout || error.outcome !== undefined
    if (expired && transmitted === false) {
      if (batch.firstSequence !== null) this.sequencer.rebase(batch)
      else this.sequencer.fail(batch)
      if (this.#transactional) this.arm(error)
      return 'FAIL_BATCH'
    }
    const { category } = this.classify(error, 'produce')
    const code = !expired && typeof error.code === 'number' ? error.code : undefined
    const verdict = this.sequencer.verdict(batch, {
      category,
      code,
      logStartOffset,
      transactional: this.#transactional,
      attemptsLeft: this.#retries - batch.attempt,
      expired,
    })
    const { topic, partition } = batch

    switch (verdict) {
      case 'COMPLETE_AS_DUPLICATE':
        return verdict
      case 'RETRY_IN_ORDER':
      case 'BUMP_EPOCH':
        if (this.#armed()) {
          this.sequencer.fail(batch)
          return 'FAIL_BATCH'
        }
        if (verdict === 'BUMP_EPOCH') {
          this.#bump(topic, partition)
        } else if (
          this.#transactional &&
          code === UNKNOWN_PRODUCER_ID &&
          this.sequencer.isTruncated(batch, logStartOffset)
        ) {
          this.sequencer.restartPartition(topic, partition)
        }
        return verdict
      case 'FAIL_BATCH':
        if (expired || (batch.attempt > 0 && this.#outOfRetries(batch, category))) {
          this.sequencer.markUnresolved(batch)
        }
        this.sequencer.fail(batch)
        if (this.#transactional && code === UNKNOWN_PRODUCER_ID) {
          this.sequencer.resetPartition(topic, partition)
        }
        if (this.#armsOnFailure(code, expired)) {
          this.arm(error)
        }
        return verdict
      case 'FAIL_BATCH_AND_REBASE':
        if (this.#transactional) {
          this.sequencer.rebase(batch)
        } else {
          this.sequencer.fail(batch)
          this.#bump(topic, partition)
        }
        return verdict
      case 'FAIL_BATCH_AND_BUMP':
        this.sequencer.fail(batch)
        this.#bump(topic, partition)
        return verdict
    }
  }

  /**
   * Registers a partition a transactional send is about to write. Under transaction version 2 it
   * joins the transaction at once - Produce v12 adds it on the broker - and the transaction counts
   * as started. Under version 1 the partitions registered in one synchronous turn go to the
   * coordinator in one AddPartitionsToTxn, retried while it fails retriably; each joins the
   * transaction, and becomes drainable, when that request succeeds, and a failure arms the state
   * its error calls for, which rejects the queued batches.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @param {string} topic
   * @param {number|string} partition
   * @returns {Promise<void>|null} the AddPartitionsToTxn carrying the partition - rejecting with
   *   its error after the state is armed - or null when the partition is already in
   */
  addPartitionToTransaction(topic, partition) {
    const id = Number(partition)
    if (this.#isPartitionInTransaction(topic, id)) {
      return null
    }

    if (this.version.current() === 2) {
      this.#join(topic, id)
      this.#started = true
      return null
    }

    const pending = this.#adding.get(topic)?.get(id)
    if (pending !== undefined) {
      return pending
    }

    if (this.#nextAdd === null) {
      this.#scheduleAdd()
    }

    if (!this.#nextAdd.has(topic)) {
      this.#nextAdd.set(topic, [])
    }
    this.#nextAdd.get(topic).push(id)
    if (!this.#adding.has(topic)) {
      this.#adding.set(topic, new Map())
    }
    this.#adding.get(topic).set(id, this.#nextAddPromise)
    return this.#nextAddPromise
  }

  /**
   * @returns {Promise<void>} resolves once every AddPartitionsToTxn started so far has settled,
   *   whatever its outcome; never rejects
   */
  whenPartitionsAdded() {
    return Promise.all(this.#adds).then(ignore)
  }

  /** @returns {boolean} whether the current transaction has a partition or offsets in it */
  transactionStarted() {
    return this.#started
  }

  /** Records that offsets joined the current transaction. @returns {void} */
  markOffsetsAdded() {
    this.#started = true
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {number} the partition's next sequence; 0 for one never used
   */
  nextSequence(topic, partition) {
    return this.sequencer.nextSequence(topic, partition)
  }

  /**
   * Moves a partition's next sequence by `delta`, with the per-broker send path's rotation.
   * @param {string} topic
   * @param {number} partition
   * @param {number} delta
   * @returns {number} the new next sequence
   */
  advance(topic, partition, delta) {
    return this.sequencer.advance(topic, partition, delta)
  }

  /**
   * The drain's view of a partition's place in the transaction: always in for a producer that is
   * not transactional.
   * @param {string} topic
   * @param {number} partition
   * @returns {boolean}
   */
  #isPartitionInTransaction = (topic, partition) =>
    !this.#transactional || this.#partitions.get(topic)?.has(Number(partition)) === true

  /**
   * Whether a batch ran out of retries on a retriable error. The broker may hold one that was
   * re-sent, so - as for an expired batch, which is where Java meets this case - its sequence is
   * left unresolved and the partition's next batch waits for the outcome; `markUnresolved` leaves
   * a batch never re-sent alone.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @param {PartitionBatch} batch
   * @param {Category} category
   * @returns {boolean}
   */
  #outOfRetries(batch, category) {
    return batch.attempt >= this.#retries && RETRIABLE_CATEGORIES.has(category)
  }

  /** @returns {boolean} whether `ABORTABLE_ERROR` or `FATAL_ERROR` is armed */
  #armed() {
    return isArmed(this.transaction.state())
  }

  /**
   * @param {number|undefined} code
   * @param {boolean} expired
   * @returns {boolean} whether a failed batch arms an error state
   */
  #armsOnFailure(code, expired) {
    if (code === INVALID_PRODUCER_ID_MAPPING) {
      return true
    }

    return (
      this.#transactional &&
      (expired || code === UNKNOWN_PRODUCER_ID || code === OUT_OF_ORDER_SEQUENCE_NUMBER)
    )
  }

  /**
   * A local epoch bump with its narrow reset: the partition that triggered it restarts at 0, the
   * others are stale until their batches in flight settle. Does nothing once `FATAL_ERROR` is
   * armed: a producer that must be replaced takes no sequence or epoch action, as in Java.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  #bump(topic, partition) {
    if (this.transaction.state() === FATAL_ERROR) {
      return
    }

    this.#identity.bumpEpoch()
    this.sequencer.restartPartition(topic, partition)
    this.sequencer.markStaleExcept(topic, partition)
  }

  /**
   * Resets the sequences for a new producer id or epoch: every partition at once when nothing is
   * in flight, otherwise each marked stale to restart once its own batches have settled.
   * @returns {void}
   */
  #resetSequences() {
    if (this.sequencer.hasInFlight()) {
      this.sequencer.markAllStale()
    } else {
      this.sequencer.resetAll()
    }
  }

  /**
   * Acquires the producer id `ensureProducerId()` found missing: retries a retriable error with
   * the producer's retry options and stops at once on any other. It also stops, without a request,
   * once any caller holds a producer id again, so one acquired during a backoff is kept. A fatal
   * error arms `FATAL_ERROR`; any other final failure rejects the queued batches with it. Once
   * the producer lifecycle it began under closes, it stops and changes no state.
   * @param {import('../generation')} [lifecycle] the producer lifecycle captured by the caller
   * @returns {Promise<void>} never rejects
   */
  async #recover(lifecycle) {
    try {
      await this.#retrierFor(lifecycle)(async (bail) => {
        lifecycle?.check()
        if (this.#identity.isValid()) {
          return
        }

        try {
          await this.initialize()
        } catch (e) {
          lifecycle?.check()
          this.classify(e, 'txn')
          if (e.retriable === true) {
            throw e
          }
          bail(e)
        }
      })
    } catch (error) {
      if (lifecycle?.closed) {
        return
      }
      if (transactionStateFor(error) === FATAL_ERROR) {
        this.arm(error)
      } else {
        this.sequencer.abortQueued(error)
      }
    }
  }

  /**
   * @param {import('../generation')} [lifecycle] the producer lifecycle an operation captured
   * @returns {import('../../retry').Retrier} a retrier that stops, with the lifecycle's close
   *   error, when the lifecycle closes; the manager's own without one
   */
  #retrierFor(lifecycle) {
    return lifecycle ? createRetry({ ...this.#retry, fence: lifecycle }) : this.#retrier
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  #join(topic, partition) {
    if (!this.#partitions.has(topic)) {
      this.#partitions.set(topic, new Set())
    }
    this.#partitions.get(topic).add(partition)
  }

  /**
   * Starts collecting the partitions of the next AddPartitionsToTxn; it is sent once the current
   * synchronous turn ends.
   * @returns {void}
   */
  #scheduleAdd() {
    const topics = new Map()
    this.#nextAdd = topics
    const generation = this.#generation
    const lifecycle = this.#getGeneration()
    this.#nextAddPromise = Promise.resolve().then(() =>
      this.#sendAdd(topics, generation, lifecycle)
    )
    const settled = this.#nextAddPromise.then(ignore, ignore)
    this.#adds.add(settled)
    settled.then(() => this.#adds.delete(settled))
  }

  /**
   * Sends one AddPartitionsToTxn for the partitions collected in `topics`, unless the transaction
   * they were registered in has ended; its answer only counts while that transaction lasts.
   * @param {Map<string, number[]>} topics
   * @param {number} generation the transaction the partitions were registered in
   * @param {import('../generation')} [lifecycle] the producer lifecycle captured when they were
   *   registered: once it closes the retries stop, and the answer joins nothing and arms nothing
   * @returns {Promise<void>}
   */
  async #sendAdd(topics, generation, lifecycle) {
    if (this.#nextAdd === topics) {
      this.#nextAdd = null
    }
    if (generation !== this.#generation) {
      return
    }

    const request = [...topics].map(([topic, partitions]) => ({ topic, partitions }))

    try {
      await this.#retrierFor(lifecycle)(async (bail) => {
        lifecycle?.check()
        try {
          await this.#addPartitionsToTxn(request, ...(lifecycle ? [lifecycle] : []))
        } catch (e) {
          lifecycle?.check()
          this.classify(e, 'txn')
          if (e.retriable === true) {
            throw e
          }
          bail(e)
        }
      })
      lifecycle?.check()

      if (generation === this.#generation) {
        request.forEach(({ topic, partitions }) => partitions.forEach((p) => this.#join(topic, p)))
        this.#started = true
      }
    } catch (error) {
      if (generation === this.#generation && !lifecycle?.closed) {
        this.arm(error)
      }
      throw error
    } finally {
      if (generation === this.#generation) {
        for (const { topic, partitions } of request) {
          const pending = this.#adding.get(topic)
          partitions.forEach((p) => pending.delete(p))
          if (pending.size === 0) {
            this.#adding.delete(topic)
          }
        }
      }
    }
  }
}

module.exports = ProducerStateManager
