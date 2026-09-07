const {
  rebalanceAction,
  rebalanceReason,
  pollIntervalExceededError,
  REBALANCE_PATHS,
} = require('../rebalanceErrorContract')
const {
  isRebalancing,
  isLostMember,
  isRevoked,
  isFencedInstance,
  isUnknownMemberId,
  isStaleCoordinator,
  isPollIntervalExceeded,
} = require('../../errors')
const { createErrorFromCode, errorCodes } = require('../../protocol/error')

const PATHS = Object.values(REBALANCE_PATHS)

describe('Consumer > rebalance error contract', () => {
  describe('path x predicate', () => {
    const ROWS = [
      [22, 'lost', false, false],
      [25, 'lost', false, true],
      [82, 'lost', true, false],
      [27, 'revoked', false, false],
      [14, 'revoked', false, false],
      [16, 'revoked', false, false],
      [15, 'retry-coordinator', false, false],
    ]

    test.each(ROWS)(
      'code %i is %s (fatal=%p, clearsMemberId=%p) on every path',
      (code, action, fatal, clearsMemberId) => {
        for (const path of PATHS) {
          expect(rebalanceAction(createErrorFromCode(code), path)).toEqual({
            action,
            fatal,
            clearsMemberId,
          })
        }
      }
    )

    it('classifies the internal poll-interval error as lost, clearing the member id', () => {
      const error = pollIntervalExceededError({ elapsedMs: 9000, maxPollInterval: 8000 })

      for (const path of PATHS) {
        expect(rebalanceAction(error, path)).toEqual({
          action: 'lost',
          fatal: false,
          clearsMemberId: true,
        })
      }
    })

    it('returns no action for an error that is not rebalance-shaped', () => {
      expect(rebalanceAction(createErrorFromCode(87), REBALANCE_PATHS.FETCH)).toEqual({
        action: null,
        fatal: false,
        clearsMemberId: false,
      })
      expect(rebalanceAction({}, REBALANCE_PATHS.HEARTBEAT).action).toBeNull()
    })

    it('throws for an unknown path, so a typo is not silently classified', () => {
      expect(() => rebalanceAction(createErrorFromCode(27), 'produce')).toThrow('produce')
    })

    it('never sees MEMBER_ID_REQUIRED, which the broker retries itself', () => {
      // Recorded as a control rather than a behaviour: Broker#joinGroup retries with the member id
      // the error carried, so code 79 does not reach this contract.
      expect(rebalanceAction(createErrorFromCode(79), REBALANCE_PATHS.MEMBERSHIP).action).toBeNull()
    })
  })

  describe('truth-table equality with the shipped predicates', () => {
    const SHAPES = [
      ...errorCodes.map(({ code }) => createErrorFromCode(code)),
      { type: 'REBALANCE_IN_PROGRESS' },
      { type: 'NOT_COORDINATOR_FOR_GROUP' },
      { type: 'ILLEGAL_GENERATION' },
      {},
    ]

    it('derives every action from the predicates rather than a second code list', () => {
      for (const error of SHAPES) {
        const { action, fatal, clearsMemberId } = rebalanceAction(error, REBALANCE_PATHS.MEMBERSHIP)

        const expected = isPollIntervalExceeded(error)
          ? 'lost'
          : isLostMember(error)
            ? 'lost'
            : isRevoked(error)
              ? 'revoked'
              : isStaleCoordinator(error)
                ? 'retry-coordinator'
                : null

        expect(action).toEqual(expected)
        expect(fatal).toEqual(isFencedInstance(error))
        expect(clearsMemberId).toEqual(
          isPollIntervalExceeded(error) || (isLostMember(error) && isUnknownMemberId(error))
        )
      }
    })

    it('keeps isRebalancing matching on the type string for three of its four cases', () => {
      expect(isRebalancing({ type: 'REBALANCE_IN_PROGRESS' })).toBe(true)
      expect(isRebalancing({ type: 'NOT_COORDINATOR_FOR_GROUP' })).toBe(true)
      expect(isRebalancing({ type: 'ILLEGAL_GENERATION' })).toBe(true)
      expect(isRebalancing({})).toBe(false)
    })
  })

  describe('rebalanceReason is total', () => {
    const REASONS = [
      [createErrorFromCode(22), 'lost'],
      [createErrorFromCode(25), 'lost'],
      [createErrorFromCode(82), 'lost'],
      [createErrorFromCode(27), 'rebalance-in-progress'],
      [{ type: 'REBALANCE_IN_PROGRESS' }, 'rebalance-in-progress'],
      [createErrorFromCode(14), 'coordinator-changed'],
      [createErrorFromCode(16), 'coordinator-changed'],
      [{ type: 'NOT_COORDINATOR_FOR_GROUP' }, 'coordinator-changed'],
    ]

    test.each(REASONS)('%p reads %s', (error, reason) => {
      expect(rebalanceReason(error)).toEqual(reason)
    })

    it('reads the internal poll error as poll-interval-exceeded', () => {
      expect(
        rebalanceReason(pollIntervalExceededError({ elapsedMs: 1, maxPollInterval: 1 }))
      ).toEqual('poll-interval-exceeded')
    })

    it('reads a type-only ILLEGAL_GENERATION as coordinator-changed, as the shipped code does', () => {
      // Kept, not fixed: isLostMember is code-only, so this shape has always taken the revoked
      // branch. Changing it here would change behaviour no decision asked to change.
      expect(rebalanceReason({ type: 'ILLEGAL_GENERATION' })).toEqual('coordinator-changed')
      expect(isLostMember({ type: 'ILLEGAL_GENERATION' })).toBe(false)
    })

    it('returns null for an error that is not a rebalance at all', () => {
      expect(rebalanceReason(createErrorFromCode(87))).toBeNull()
    })

    it('reads code 15 as coordinator-changed, while its ACTION stays retry-coordinator', () => {
      // `isStaleCoordinator` matches 15 and 16; `isRebalancing` matches only 16 of the two. The
      // reason map is deliberately wider than the action map: a 15 that reaches a round (through
      // the membership path) still reports why it rejoined, while a 15 on the heartbeat path
      // raises no verdict at all, so `rebalanceReason` is never consulted for it.
      expect(rebalanceReason(createErrorFromCode(15))).toEqual('coordinator-changed')
      expect(rebalanceAction(createErrorFromCode(15), REBALANCE_PATHS.HEARTBEAT).action).toEqual(
        'retry-coordinator'
      )
    })
  })

  describe('the internal poll-interval error', () => {
    it('is retriable false, carries its numbers, and is not exported from the package', () => {
      const error = pollIntervalExceededError({ elapsedMs: 9000, maxPollInterval: 8000 })

      expect(error.type).toEqual('POLL_INTERVAL_EXCEEDED')
      expect(error.retriable).toBe(false)
      expect(error.elapsedMs).toEqual(9000)
      expect(error.maxPollInterval).toEqual(8000)
      expect(isPollIntervalExceeded(error)).toBe(true)
      expect(isPollIntervalExceeded(createErrorFromCode(27))).toBe(false)

      const publicApi = require('../../../index')
      expect(Object.keys(publicApi)).not.toContain('KafkaJSPollIntervalExceeded')
      // And the predicate itself stays internal, like the six beside it.
      expect(Object.keys(publicApi)).not.toContain('isPollIntervalExceeded')
    })
  })
})
