const { KafkaJSInvariantViolation } = require('../../errors')

/**
 * The ten states a KIP-848 member can be in, named as the Java client names them.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/MemberState.java
 */
const MEMBER_STATES = Object.freeze({
  UNSUBSCRIBED: 'UNSUBSCRIBED',
  JOINING: 'JOINING',
  RECONCILING: 'RECONCILING',
  ACKNOWLEDGING: 'ACKNOWLEDGING',
  STABLE: 'STABLE',
  PREPARE_LEAVING: 'PREPARE_LEAVING',
  LEAVING: 'LEAVING',
  FENCED: 'FENCED',
  FATAL: 'FATAL',
  STALE: 'STALE',
})

const {
  UNSUBSCRIBED,
  JOINING,
  RECONCILING,
  ACKNOWLEDGING,
  STABLE,
  PREPARE_LEAVING,
  LEAVING,
  FENCED,
  FATAL,
  STALE,
} = MEMBER_STATES

/**
 * Every state's legal predecessors, copied from `MemberState.previousValidStates` - with the one
 * documented widening noted on the `FATAL` row. This object is the machine: `transition` is a
 * lookup in it and the assignment predicate is derived from it, so a row cannot change without
 * both changing with it.
 * @type {Readonly<Record<string, string[]>>}
 */
const PREVIOUS_VALID_STATES = Object.freeze({
  [STABLE]: Object.freeze([JOINING, ACKNOWLEDGING, RECONCILING]),
  [RECONCILING]: Object.freeze([STABLE, JOINING, ACKNOWLEDGING, RECONCILING]),
  [ACKNOWLEDGING]: Object.freeze([RECONCILING]),
  // FENCED and STALE are this client's two additions to Java's row. Java only ever learns a fatal
  // error from a heartbeat response, and a fenced or stale member sends none; this client also
  // routes a commit or a fetch failure through `onMembershipError`, so it can learn of an
  // unsupported assignor while fenced. Without the two edges that error would be recorded and
  // never acted on, and the member would rejoin straight back into it.
  // @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/MemberState.java
  [FATAL]: Object.freeze([
    JOINING,
    STABLE,
    RECONCILING,
    ACKNOWLEDGING,
    PREPARE_LEAVING,
    LEAVING,
    UNSUBSCRIBED,
    FENCED,
    STALE,
  ]),
  [FENCED]: Object.freeze([JOINING, STABLE, RECONCILING, ACKNOWLEDGING, PREPARE_LEAVING, LEAVING]),
  [JOINING]: Object.freeze([FENCED, UNSUBSCRIBED, STALE]),
  [PREPARE_LEAVING]: Object.freeze([JOINING, STABLE, RECONCILING, ACKNOWLEDGING, UNSUBSCRIBED]),
  [LEAVING]: Object.freeze([PREPARE_LEAVING]),
  [UNSUBSCRIBED]: Object.freeze([PREPARE_LEAVING, LEAVING, FENCED]),
  [STALE]: Object.freeze([LEAVING]),
})

/**
 * Moves the machine, or refuses.
 *
 * A transition into the state the machine is already in is always legal, whether or not the state
 * lists itself as its own predecessor: `MemberState.transitionTo()` tests
 * `!state.equals(nextState) && !nextState.getPreviousValidStates().contains(state)`, so a
 * self-transition never reaches the table. That is what makes re-entering FENCED or FATAL - which
 * happens when a commit or a fetch learns of the same fencing the heartbeat already handled -
 * idempotent instead of a crash.
 *
 * An illegal edge is a bug in this client rather than anything a broker did, so it raises the
 * invariant-violation class: it is non-retriable, which keeps `onCrash` from restarting straight
 * back into the same edge, and it names both states so the offending caller is identifiable from
 * the message alone.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractMembershipManager.java
 * @param {string} from the current state
 * @param {string} to the wanted state
 * @returns {string} `to`, so callers can write `this.state = transition(this.state, TARGET)`
 * @throws {KafkaJSInvariantViolation} when either state is unknown, or the edge is not in the table
 */
const transition = (from, to) => {
  const allowed = PREVIOUS_VALID_STATES[to]

  if (!allowed || !Object.values(MEMBER_STATES).includes(from)) {
    throw new KafkaJSInvariantViolation(`unknown member state transition ${from} -> ${to}`)
  }

  if (from === to) {
    return to
  }

  if (!allowed.includes(from)) {
    throw new KafkaJSInvariantViolation(
      `illegal member state transition ${from} -> ${to}; ${to} may only follow ${allowed.join(', ')}`
    )
  }

  return to
}

/**
 * Whether a heartbeat's target assignment may be taken in this state. Java's definition is
 * literally "is a legal predecessor of RECONCILING", so it is read from the table rather than
 * written out again; an assignment arriving in any other state is dropped.
 * @param {string} state
 * @returns {boolean}
 */
const canHandleNewAssignment = (state) => PREVIOUS_VALID_STATES[RECONCILING].includes(state)

const HEARTBEAT_NOW_STATES = Object.freeze([ACKNOWLEDGING, LEAVING, JOINING])
const SKIP_HEARTBEAT_STATES = Object.freeze([UNSUBSCRIBED, FENCED, FATAL, STALE])
const NOT_IN_GROUP_STATES = Object.freeze([UNSUBSCRIBED, FENCED, FATAL, STALE])
const LEAVING_STATES = Object.freeze([PREPARE_LEAVING, LEAVING])

/**
 * The three states whose heartbeat must go out now rather than on the interval: the join, the ack
 * and the leave.
 * @param {string} state
 * @returns {boolean}
 */
const shouldHeartbeatNow = (state) => HEARTBEAT_NOW_STATES.includes(state)

/**
 * The states that send no heartbeat at all. The same four states as `isNotInGroup` today, but
 * backed by its own list: the two names answer different questions - one gates the heartbeat, the
 * other gates the rejoin - and sharing one constant would make a future divergence impossible to
 * express without touching both call sites at once.
 * @param {string} state
 * @returns {boolean}
 */
const shouldSkipHeartbeat = (state) => SKIP_HEARTBEAT_STATES.includes(state)

/**
 * Whether this member is not part of the group, so a rejoin has to run before anything else.
 * @param {string} state
 * @returns {boolean}
 */
const isNotInGroup = (state) => NOT_IN_GROUP_STATES.includes(state)

/**
 * Whether a leave is already under way, so a second one joins it rather than starting another.
 * @param {string} state
 * @returns {boolean}
 */
const isLeavingGroup = (state) => LEAVING_STATES.includes(state)

module.exports = {
  MEMBER_STATES,
  PREVIOUS_VALID_STATES,
  transition,
  canHandleNewAssignment,
  shouldHeartbeatNow,
  shouldSkipHeartbeat,
  isNotInGroup,
  isLeavingGroup,
}
