const {
  MEMBER_STATES,
  PREVIOUS_VALID_STATES,
  transition,
  canHandleNewAssignment,
  shouldHeartbeatNow,
  shouldSkipHeartbeat,
  isLeavingGroup,
  isNotInGroup,
} = require('../groupProtocol/memberState')
const { KafkaJSInvariantViolation } = require('../../errors')

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

// The table, retyped from MemberState.java rather than imported, so a typo in the module is a
// failing test rather than a test that agrees with the typo. The FATAL row carries this client's
// one documented widening: FENCED and STALE are legal predecessors here and not in Java, because
// this client can learn a fatal error on the commit or fetch path while the member is out of the
// group, where Java only ever learns one from a heartbeat response.
// @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/MemberState.java
const TABLE = {
  [STABLE]: [JOINING, ACKNOWLEDGING, RECONCILING],
  [RECONCILING]: [STABLE, JOINING, ACKNOWLEDGING, RECONCILING],
  [ACKNOWLEDGING]: [RECONCILING],
  [FATAL]: [
    JOINING,
    STABLE,
    RECONCILING,
    ACKNOWLEDGING,
    PREPARE_LEAVING,
    LEAVING,
    UNSUBSCRIBED,
    FENCED,
    STALE,
  ],
  [FENCED]: [JOINING, STABLE, RECONCILING, ACKNOWLEDGING, PREPARE_LEAVING, LEAVING],
  [JOINING]: [FENCED, UNSUBSCRIBED, STALE],
  [PREPARE_LEAVING]: [JOINING, STABLE, RECONCILING, ACKNOWLEDGING, UNSUBSCRIBED],
  [LEAVING]: [PREPARE_LEAVING],
  [UNSUBSCRIBED]: [PREPARE_LEAVING, LEAVING, FENCED],
  [STALE]: [LEAVING],
}

const ALL = Object.values(MEMBER_STATES)

const legalEdges = () =>
  Object.entries(TABLE).flatMap(([to, froms]) => froms.map((from) => [from, to]))

// A self-transition is legal for every state, whether or not the state lists itself as its own
// predecessor: MemberState.transitionTo() tests `!state.equals(nextState) && !previousValidStates
// .contains(state)`, so an edge to the state the machine is already in never reaches the table.
const selfEdges = () => ALL.map((state) => [state, state])

const illegalEdges = () =>
  ALL.flatMap((from) => ALL.map((to) => [from, to])).filter(
    ([from, to]) => from !== to && !(TABLE[to] || []).includes(from)
  )

describe('Consumer > groupProtocol > memberState', () => {
  it('ships the predecessor table verbatim', () => {
    expect(Object.keys(PREVIOUS_VALID_STATES).sort()).toEqual(Object.keys(TABLE).sort())
    for (const [state, froms] of Object.entries(TABLE)) {
      expect([...PREVIOUS_VALID_STATES[state]].sort()).toEqual([...froms].sort())
    }
  })

  it('freezes the table, so no caller can widen the machine at runtime', () => {
    expect(Object.isFrozen(MEMBER_STATES)).toBe(true)
    expect(Object.isFrozen(PREVIOUS_VALID_STATES)).toBe(true)
  })

  test.each(legalEdges())('allows %s -> %s and returns the target state', (from, to) => {
    expect(transition(from, to)).toEqual(to)
  })

  test.each(illegalEdges())('throws on %s -> %s', (from, to) => {
    expect(() => transition(from, to)).toThrow(KafkaJSInvariantViolation)
    expect(() => transition(from, to)).toThrow(`${from}`)
    expect(() => transition(from, to)).toThrow(`${to}`)
  })

  test.each(selfEdges())('allows the self-transition %s -> %s', (from, to) => {
    expect(transition(from, to)).toEqual(to)
  })

  it('counts the edges, so a row silently deleted from the table fails here', () => {
    // 100 ordered pairs: 36 the table allows (one of which, RECONCILING -> RECONCILING, is also a
    // self-edge), 9 further self-edges the table does not list but the machine still allows, and
    // 55 genuinely forbidden.
    expect(legalEdges()).toHaveLength(36)
    expect(selfEdges()).toHaveLength(10)
    expect(illegalEdges()).toHaveLength(55)
    expect(legalEdges().length + illegalEdges().length + selfEdges().length).toEqual(
      ALL.length * ALL.length + 1
    )
  })

  it('allows a repeated transition into the state the machine is already in', () => {
    // The fencing path can be entered twice - once from the heartbeat response, once from a commit
    // or fetch that learns of the same fencing - and the second entry must be a no-op rather than
    // an invariant violation.
    expect(transition(FENCED, FENCED)).toEqual(FENCED)
    expect(transition(RECONCILING, RECONCILING)).toEqual(RECONCILING)
    expect(transition(FATAL, FATAL)).toEqual(FATAL)
  })

  it('throws on an unknown state on either side', () => {
    expect(() => transition('WOBBLING', STABLE)).toThrow(KafkaJSInvariantViolation)
    expect(() => transition(STABLE, 'WOBBLING')).toThrow(KafkaJSInvariantViolation)
  })

  describe('predicates', () => {
    const ROWS = [
      [canHandleNewAssignment, [STABLE, JOINING, ACKNOWLEDGING, RECONCILING]],
      [shouldHeartbeatNow, [ACKNOWLEDGING, LEAVING, JOINING]],
      [shouldSkipHeartbeat, [UNSUBSCRIBED, FATAL, STALE, FENCED]],
      [isLeavingGroup, [PREPARE_LEAVING, LEAVING]],
      [isNotInGroup, [UNSUBSCRIBED, FENCED, FATAL, STALE]],
    ]

    test.each(ROWS)('%p is true for exactly its own states', (predicate, states) => {
      for (const state of ALL) {
        expect(predicate(state)).toBe(states.includes(state))
      }
    })

    it('derives canHandleNewAssignment from the table rather than a second list', () => {
      // Java's definition is literally "is a legal predecessor of RECONCILING".
      for (const state of ALL) {
        expect(canHandleNewAssignment(state)).toBe(
          PREVIOUS_VALID_STATES[RECONCILING].includes(state)
        )
      }
    })

    it('keeps shouldSkipHeartbeat and isNotInGroup as two questions that agree today', () => {
      // Each predicate is backed by its own list in the module, so one can change without the
      // other; this case records that today they answer the same, and fails loudly if that stops
      // being true without the ROWS table above being updated first.
      for (const state of ALL) {
        expect(shouldSkipHeartbeat(state)).toBe(isNotInGroup(state))
      }
    })
  })
})
