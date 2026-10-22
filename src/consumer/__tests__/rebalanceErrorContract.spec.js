const {
  rebalanceAction,
  rebalanceReason,
  pollIntervalExceededError,
  stampOffsetCommit,
  isDeletedGroupCommit,
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
      [16, 'retry-coordinator', false, false],
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

  // Apache Kafka's OffsetMetadataManager.validateOffsetCommit answers GROUP_ID_NOT_FOUND
  // (69) at OffsetCommit v9+ where v8 answered ILLEGAL_GENERATION (22). A member must not tell
  // those two apart, and nothing else about 69 may change.
  describe('a vanished group answering an OffsetCommit', () => {
    const committed = (code) => stampOffsetCommit(createErrorFromCode(code))

    it.each([['classic'], ['consumer']])(
      'takes the ILLEGAL_GENERATION verdict under %s',
      (kind) => {
        const asIllegalGeneration = rebalanceAction(
          createErrorFromCode(22),
          REBALANCE_PATHS.FETCH,
          {
            kind,
          }
        )

        for (const path of PATHS) {
          expect(rebalanceAction(committed(69), path, { kind })).toEqual(asIllegalGeneration)
        }

        expect(rebalanceReason(committed(69), { kind })).toEqual(
          rebalanceReason(createErrorFromCode(22), { kind })
        )
      }
    )

    it('keeps the member id, which a vanished group does not invalidate', () => {
      expect(rebalanceAction(committed(69), REBALANCE_PATHS.OFFSET_COMMIT)).toEqual({
        action: 'lost',
        fatal: false,
        clearsMemberId: false,
      })
    })

    it('leaves an unstamped 69 alone, on every path and under both protocols', () => {
      for (const kind of ['classic', 'consumer']) {
        for (const path of PATHS) {
          expect(rebalanceAction(createErrorFromCode(69), path, { kind }).action).toBeNull()
        }

        expect(rebalanceReason(createErrorFromCode(69), { kind })).toBeNull()
      }

      expect(isDeletedGroupCommit(createErrorFromCode(69))).toBe(false)
    })

    it('leaves every other stamped code alone', () => {
      expect(isDeletedGroupCommit(committed(22))).toBe(false)
      expect(rebalanceAction(committed(27), REBALANCE_PATHS.FETCH)).toEqual(
        rebalanceAction(createErrorFromCode(27), REBALANCE_PATHS.FETCH)
      )
    })

    it('does not make 69 retriable, so admin reads still fail at once', () => {
      // `admin.fetchOffsets` and the describe router both read this flag; the stamp is a
      // classification input for the consumer, not a change to the error table.
      expect(createErrorFromCode(69).retriable).toBe(false)
      expect(committed(69).retriable).toBe(false)
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
            : isStaleCoordinator(error)
              ? 'retry-coordinator'
              : isRevoked(error)
                ? 'revoked'
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

  describe('the membership protocol changes three rows and one reason', () => {
    const CONSUMER = { kind: 'consumer' }

    const ROWS = [
      [
        14,
        { action: 'revoked', fatal: false, clearsMemberId: false },
        { action: null, fatal: false, clearsMemberId: false },
      ],
      [
        15,
        { action: 'retry-coordinator', fatal: false, clearsMemberId: false },
        { action: 'retry-coordinator', fatal: false, clearsMemberId: false },
      ],
      [
        16,
        { action: 'retry-coordinator', fatal: false, clearsMemberId: false },
        { action: 'retry-coordinator', fatal: false, clearsMemberId: false },
      ],
      [
        25,
        { action: 'lost', fatal: false, clearsMemberId: true },
        { action: 'lost', fatal: false, clearsMemberId: false },
      ],
      [
        110,
        { action: 'lost', fatal: false, clearsMemberId: false },
        { action: 'lost', fatal: false, clearsMemberId: false },
      ],
      [
        82,
        { action: 'lost', fatal: true, clearsMemberId: false },
        { action: 'lost', fatal: true, clearsMemberId: false },
      ],
      [
        81,
        { action: null, fatal: false, clearsMemberId: false },
        { action: 'lost', fatal: true, clearsMemberId: false },
      ],
      [
        111,
        { action: null, fatal: false, clearsMemberId: false },
        { action: 'lost', fatal: true, clearsMemberId: false },
      ],
      [
        112,
        { action: null, fatal: false, clearsMemberId: false },
        { action: 'lost', fatal: true, clearsMemberId: false },
      ],
      [
        128,
        { action: null, fatal: false, clearsMemberId: false },
        { action: 'lost', fatal: true, clearsMemberId: false },
      ],
      [
        42,
        { action: null, fatal: false, clearsMemberId: false },
        { action: 'lost', fatal: true, clearsMemberId: false },
      ],
      [
        30,
        { action: null, fatal: false, clearsMemberId: false },
        { action: 'lost', fatal: true, clearsMemberId: false },
      ],
      [
        113,
        { action: null, fatal: false, clearsMemberId: false },
        { action: null, fatal: false, clearsMemberId: false },
      ],
    ]

    test.each(ROWS)(
      'code %i classifies as %p classically and %p under the consumer protocol',
      (code, classic, consumer) => {
        for (const path of PATHS) {
          expect(rebalanceAction(createErrorFromCode(code), path)).toEqual(classic)
          expect(rebalanceAction(createErrorFromCode(code), path, { kind: 'classic' })).toEqual(
            classic
          )
          expect(rebalanceAction(createErrorFromCode(code), path, CONSUMER)).toEqual(consumer)
        }
      }
    )

    it('never asks the consumer protocol to clear a member id', () => {
      // The member id is generated once per membership and survives every fencing, so no row may
      // answer `true` here - 25, which does clear it classically, included.
      for (const [code] of ROWS) {
        for (const path of PATHS) {
          expect(rebalanceAction(createErrorFromCode(code), path, CONSUMER).clearsMemberId).toBe(
            false
          )
        }
      }
    })

    const REASONS = [
      [25, 'lost', 'fenced'],
      [110, 'lost', 'fenced'],
      [82, 'lost', 'lost'],
      [22, 'lost', 'lost'],
      [112, null, 'lost'],
      [27, 'rebalance-in-progress', 'rebalance-in-progress'],
      [16, 'coordinator-changed', 'coordinator-changed'],
    ]

    test.each(REASONS)(
      'code %i reads %p classically and %p under the consumer protocol',
      (code, classic, consumer) => {
        expect(rebalanceReason(createErrorFromCode(code))).toEqual(classic)
        expect(rebalanceReason(createErrorFromCode(code), { kind: 'classic' })).toEqual(classic)
        expect(rebalanceReason(createErrorFromCode(code), { kind: 'consumer' })).toEqual(consumer)
      }
    )

    it('never derives assignment-changed from an error; the caller supplies it', () => {
      const { errorCodes } = require('../../protocol/error')

      for (const { code } of errorCodes) {
        expect(rebalanceReason(createErrorFromCode(code), { kind: 'consumer' })).not.toEqual(
          'assignment-changed'
        )
      }
    })

    it('keeps the reason map total under the consumer protocol', () => {
      for (const { code } of require('../../protocol/error').errorCodes) {
        const error = createErrorFromCode(code)
        const { action } = rebalanceAction(error, REBALANCE_PATHS.HEARTBEAT, { kind: 'consumer' })

        if (action === 'lost' || action === 'revoked') {
          expect(rebalanceReason(error, { kind: 'consumer' })).not.toBeNull()
        }
      }
    })
  })

  describe('the consumer-protocol fatal set', () => {
    const { isConsumerProtocolFatal } = require('../rebalanceErrorContract')
    const { isFatalMembership } = require('../../errors')

    test.each([[81], [111], [112], [128], [82], [42], [30]])(
      'code %i is fatal to the membership',
      (code) => {
        expect(isConsumerProtocolFatal(createErrorFromCode(code))).toBe(true)
      }
    )

    test.each([[14], [15], [16], [25], [110], [113], [27], [22]])('code %i is not', (code) => {
      expect(isConsumerProtocolFatal(createErrorFromCode(code))).toBe(false)
    })

    it('keeps 30 and 42 out of the exported predicate, so the classic path is untouched', () => {
      expect(isFatalMembership(createErrorFromCode(30))).toBe(false)
      expect(isFatalMembership(createErrorFromCode(42))).toBe(false)
      expect(isFatalMembership(createErrorFromCode(35))).toBe(false)
    })
  })

  describe('the internal errors the membership and the join paths carry', () => {
    const {
      assignmentChangedError,
      consumerStoppingError,
      joinDeadlineError,
      unsupportedConsumerProtocolError,
    } = require('../rebalanceErrorContract')
    const { isFatalMembership } = require('../../errors')

    it('are shaped so the verdict path does what each one needs', () => {
      expect(assignmentChangedError()).toMatchObject({
        type: 'ASSIGNMENT_CHANGED',
        retriable: false,
      })
      expect(consumerStoppingError()).toMatchObject({ type: 'CONSUMER_STOPPING', retriable: false })
      expect(joinDeadlineError({ rebalanceTimeoutMs: 1000 })).toMatchObject({
        type: 'JOIN_DEADLINE_EXCEEDED',
        retriable: true,
      })
      expect(joinDeadlineError({ rebalanceTimeoutMs: 1000 }).message).toContain('1000')
    })

    it('classify as nothing, so none of them can raise a rebalance on its own', () => {
      for (const error of [
        assignmentChangedError(),
        consumerStoppingError(),
        joinDeadlineError({ rebalanceTimeoutMs: 1 }),
      ]) {
        expect(
          rebalanceAction(error, REBALANCE_PATHS.HEARTBEAT, { kind: 'consumer' }).action
        ).toBeNull()
        expect(rebalanceReason(error, { kind: 'consumer' })).toBeNull()
      }
    })

    it('makes the unsupported-protocol error fatal, and says what to set instead', () => {
      const error = unsupportedConsumerProtocolError(createErrorFromCode(35))

      expect(error.message).toEqual(
        "The cluster does not support the new CONSUMER group protocol. Set groupProtocol: 'classic' on the consumer until the cluster is upgraded."
      )
      expect(isFatalMembership(error)).toBe(true)
      expect(error.retriable).toBe(false)
      expect(rebalanceAction(error, REBALANCE_PATHS.HEARTBEAT, { kind: 'consumer' })).toEqual({
        action: 'lost',
        fatal: true,
        clearsMemberId: false,
      })
    })

    it('is not a public export, and neither is the new predicate', () => {
      const publicApi = require('../../../index')

      expect(Object.keys(publicApi)).not.toContain('isFatalMembership')
      expect(Object.keys(publicApi)).not.toContain('CONSUMER_PROTOCOL_UNSUPPORTED')
    })
  })
})
