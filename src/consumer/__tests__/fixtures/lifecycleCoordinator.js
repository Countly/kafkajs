const RebalanceCoordinator = require('../../rebalanceCoordinator')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { events } = require('../../instrumentationEvents')
const PollTimer = require('../../pollTimer')
const { EAGER } = require('../../groupProtocol/interface')
const { newLogger } = require('testHelpers')

/**
 * Builds deterministic coordinator collaborators without opening a connection or timer.
 * @param {object} [options]
 * @param {string} [options.mode] eager or cooperative rebalance protocol
 * @param {object[]} [options.assignment] currently owned partitions
 * @param {object[]} [options.deltas] queued apply results, consumed once each
 * @param {number} [options.heartbeatInterval] membership cadence in milliseconds
 * @param {() => void} [options.onJoinComplete] observed completion callback
 * @param {object} [options.channels] optional instrumentation channels
 * @returns {object} coordinator, group, managers, ordered calls and emitted payloads
 */
const createCoordinator = ({
  mode = EAGER,
  assignment = [],
  deltas = [],
  heartbeatInterval = 3000,
  onJoinComplete,
  channels,
} = {}) => {
  const calls = []

  const consumerGroup = {
    groupId: 'coordinator-group',
    memberId: 'member-1',
    generationId: 3,
    leaderId: 'member-1',
    groupInstanceId: null,
    groupProtocol: 'RoundRobinAssigner',
    kind: 'classic',
    groupMembership: {},
    isLeader: () => true,
    rebalanceProtocol: () => mode,
    assigned: jest.fn(() => assignment),
    joinAndSync: jest.fn(async () => {
      calls.push('joinAndSync')
      return { assignment: {} }
    }),
    // A QUEUE, not a constant: a non-empty `revoked` schedules a follow-up round, so a constant
    // delta chains rounds until the round guard rejects. `deltas` is shifted one per call and
    // falls back to an empty delta, which is what makes a follow-up round converge.
    applyRebalanceResult: jest.fn(async () => {
      calls.push('applyRebalanceResult')
      return { assignment, added: [], revoked: [], ...(deltas.shift() || {}) }
    }),
    forgetOwnedPartitions: jest.fn(() => calls.push('forgetOwnedPartitions')),
    heartbeat: jest.fn(async () => calls.push('heartbeat')),
    discardUncommittedOffsets: jest.fn(() => calls.push('discard')),
    leave: jest.fn(async () => calls.push('leave')),
    resetGenerationLocally: jest.fn(() => calls.push('resetGenerationLocally')),
    clearStaleAssignmentPending: jest.fn(),
    // The membership seam the runner drives: the real ConsumerGroup answers all three. Under the
    // classic protocol the interval is the configured one, which is what the cases building a
    // real HeartbeatManager rely on.
    heartbeatInterval: () => heartbeatInterval,
    onStopRequested: jest.fn(),
    onMembershipError: jest.fn(() => calls.push('onMembershipError')),
  }

  const fetchManager = {
    start: jest.fn(async () => {}),
    stop: jest.fn(async () => calls.push('fetchManager.stop')),
    getFetchers: jest.fn(() => []),
  }

  const heartbeatManager = {
    ensureStarted: jest.fn(() => calls.push('ensureStarted')),
    stop: jest.fn(() => calls.push('heartbeatManager.stop')),
    shutdown: jest.fn(() => calls.push('heartbeatManager.shutdown')),
  }

  const runner = { stopRequested: false, onCrash: jest.fn() }
  const instrumentationEmitter = new InstrumentationEventEmitter({ channels })
  const emitted = []
  for (const name of [
    events.REBALANCING,
    events.GROUP_JOIN,
    events.PARTITIONS_ASSIGNED,
    events.PARTITIONS_REVOKED,
    events.PARTITIONS_LOST,
    events.POLL_INTERVAL_EXCEEDED,
  ]) {
    instrumentationEmitter.addListener(name, ({ type, payload }) => {
      calls.push(type)
      emitted.push({ type, payload })
    })
  }

  const coordinator = new RebalanceCoordinator({
    runner,
    consumerGroup,
    fetchManager,
    heartbeatManager,
    pollTimer: new PollTimer({ pollDeadline: 8000 }),
    instrumentationEmitter,
    logger: newLogger(),
    groupId: 'coordinator-group',
    onJoinComplete,
  })

  return {
    coordinator,
    consumerGroup,
    fetchManager,
    heartbeatManager,
    runner,
    instrumentationEmitter,
    calls,
    emitted,
  }
}

module.exports = { createCoordinator }
