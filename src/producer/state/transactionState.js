const {
  KafkaJSApplicationRecoverableError,
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const {
  UNINITIALIZED,
  READY,
  IN_TRANSACTION,
  COMMITTING,
  ABORTING,
  ABORTABLE_ERROR,
  FATAL_ERROR,
} = require('../eosManager/transactionStates')

/** The edges `transitionTo()` takes. The two error states are entered only by arming. */
const TRANSITIONS = {
  [UNINITIALIZED]: [READY],
  [READY]: [READY, IN_TRANSACTION],
  [IN_TRANSACTION]: [COMMITTING, ABORTING],
  [COMMITTING]: [READY],
  [ABORTING]: [READY],
  [ABORTABLE_ERROR]: [ABORTING],
  [FATAL_ERROR]: [],
}

/** Where each guarded call is legal; in an error state `guard()` decides first. */
const LEGAL_STATES = {
  initProducerId: [UNINITIALIZED, READY],
  beginTransaction: [READY],
  addPartitionsToTransaction: [IN_TRANSACTION],
  sendOffsets: [IN_TRANSACTION],
  commit: [IN_TRANSACTION],
  abort: [IN_TRANSACTION, ABORTABLE_ERROR],
}

/**
 * The transactional producer's state machine: seven states, the calls each one admits, and the
 * error that put it in an error state.
 *
 * `ABORTABLE_ERROR` admits `abort()` alone; every other call throws the armed error, a
 * `KafkaJSTransactionAbortableError`. `FATAL_ERROR` is terminal: every call throws the armed
 * `KafkaJSApplicationRecoverableError`, which tells the caller to create a new producer.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1050%3A+Consistent+error+handling+for+Transactions
 */
class TransactionState {
  #state = UNINITIALIZED
  #armed = null

  /**
   * @returns {import('../eosManager/transactionStates').State}
   */
  state() {
    return this.#state
  }

  /**
   * Moves along one of the machine's edges, and forgets any armed error.
   * @param {import('../eosManager/transactionStates').State} state
   * @returns {void}
   * @throws {KafkaJSNonRetriableError} for any other edge, naming both states; the error states
   *   are reached through `armAbortable()` and `armFatal()`, never here
   */
  transitionTo(state) {
    if (!TRANSITIONS[this.#state].includes(state)) {
      throw new KafkaJSNonRetriableError(
        `Transaction state exception: Invalid transition ${this.#state} --> ${state}`
      )
    }

    this.#state = state
    this.#armed = null
  }

  /**
   * Checks that a call is legal now.
   * @param {'initProducerId'|'beginTransaction'|'addPartitionsToTransaction'|'sendOffsets'|'commit'|'abort'} method
   * @returns {void}
   * @throws {KafkaJSApplicationRecoverableError} the armed error, in `FATAL_ERROR`
   * @throws {KafkaJSTransactionAbortableError} the armed error, for anything but `abort` in
   *   `ABORTABLE_ERROR`
   * @throws {KafkaJSNonRetriableError} when the call is not legal in the current state, naming it
   * @throws {KafkaJSInvariantViolation} for a method the machine does not guard
   */
  guard(method) {
    if (!Object.hasOwn(LEGAL_STATES, method)) {
      throw new KafkaJSInvariantViolation(`No transaction state guard for "${method}"`)
    }

    if (this.#state === FATAL_ERROR || (this.#state === ABORTABLE_ERROR && method !== 'abort')) {
      throw this.#armed
    }

    if (!LEGAL_STATES[method].includes(this.#state)) {
      throw new KafkaJSNonRetriableError(
        `Transaction state exception: Cannot call "${method}" in state "${this.#state}"`
      )
    }
  }

  /**
   * Arms `ABORTABLE_ERROR` from inside a transaction - `IN_TRANSACTION`, or `COMMITTING` when the
   * commit itself fails. The armed error is `error` when it is already a
   * `KafkaJSTransactionAbortableError`, else one wrapping it whose message says to abort. Once
   * armed, a later error changes nothing. While the transaction is being aborted arming does
   * nothing, as in the Java client. In `FATAL_ERROR` it does nothing too, which is this client's
   * own choice: the Java client throws `IllegalStateException` there.
   * @param {Error} error
   * @returns {void}
   * @throws {KafkaJSNonRetriableError} outside a transaction, where there is nothing to abort
   */
  armAbortable(error) {
    if (
      this.#state === ABORTABLE_ERROR ||
      this.#state === ABORTING ||
      this.#state === FATAL_ERROR
    ) {
      return
    }

    if (this.#state !== IN_TRANSACTION && this.#state !== COMMITTING) {
      throw new KafkaJSNonRetriableError(
        `Transaction state exception: Invalid transition ${this.#state} --> ${ABORTABLE_ERROR}`
      )
    }

    this.#armed =
      error instanceof KafkaJSTransactionAbortableError
        ? error
        : new KafkaJSTransactionAbortableError(
            `The transaction can no longer be committed (${error.message}); call abort() on it before starting another`,
            { cause: error }
          )
    this.#state = ABORTABLE_ERROR
  }

  /**
   * Arms `FATAL_ERROR`, from any state, for good. The armed error is `error` when it is already a
   * `KafkaJSApplicationRecoverableError`, else one wrapping it whose message says to create a new
   * producer. Arming again keeps the first error.
   * @param {Error} error
   * @returns {void}
   */
  armFatal(error) {
    if (this.#state === FATAL_ERROR) {
      return
    }

    this.#armed =
      error instanceof KafkaJSApplicationRecoverableError
        ? error
        : new KafkaJSApplicationRecoverableError(
            `The producer can no longer be used (${error.message}); create a new producer`,
            { cause: error }
          )
    this.#state = FATAL_ERROR
  }

  /**
   * @returns {Error|null} the error the current error state was armed with; null outside them
   */
  armedError() {
    return this.#armed
  }
}

module.exports = TransactionState
