/**
 * The transaction states.
 * @typedef {'UNINITIALIZED'|'READY'|'IN_TRANSACTION'|'COMMITTING'|'ABORTING'|'ABORTABLE_ERROR'|'FATAL_ERROR'} State
 */
const STATES = {
  UNINITIALIZED: 'UNINITIALIZED',
  READY: 'READY',
  IN_TRANSACTION: 'IN_TRANSACTION',
  COMMITTING: 'COMMITTING',
  ABORTING: 'ABORTING',
  ABORTABLE_ERROR: 'ABORTABLE_ERROR',
  FATAL_ERROR: 'FATAL_ERROR',
}

/**
 * @param {State} state
 * @returns {boolean} whether an error state is armed: the transaction can only be aborted, or the
 *   producer has to be replaced
 */
const isArmed = (state) => state === STATES.ABORTABLE_ERROR || state === STATES.FATAL_ERROR

module.exports = { ...STATES, isArmed }
