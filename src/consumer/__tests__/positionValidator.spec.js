const PositionValidator = require('../positionValidator')
const Long = require('../../utils/long')
const { newLogger } = require('testHelpers')

const TOPIC = 'test-topic'

describe('Consumer > PositionValidator', () => {
  const create = () => new PositionValidator({ logger: newLogger() })
  const position = (offset, trackedEpoch) => ({ offset, trackedEpoch })

  describe('request', () => {
    it('answers awaiting when both epochs are known and the leader epoch moved', () => {
      const validator = create()

      expect(
        validator.request(TOPIC, 0, { offset: '10', trackedEpoch: 4, currentLeaderEpoch: 5 })
      ).toEqual({ action: 'awaiting', generation: expect.any(Number) })
      expect(validator.isAwaiting(TOPIC, 0)).toBe(true)
      expect(validator.validatedAgainst(TOPIC, 0)).toBeUndefined()
    })

    it('a partition never validated counts as changed, so the first round validates', () => {
      const validator = create()
      expect(validator.validatedAgainst(TOPIC, 0)).toBeUndefined()
      expect(
        validator.request(TOPIC, 0, { offset: '10', trackedEpoch: 4, currentLeaderEpoch: 4 }).action
      ).toEqual('awaiting')
    })

    it('answers unchanged, writing nothing, once validated against the same epoch', () => {
      const validator = create()
      validator.markValidated(TOPIC, 0, 5)

      expect(
        validator.request(TOPIC, 0, { offset: '10', trackedEpoch: 4, currentLeaderEpoch: 5 }).action
      ).toEqual('unchanged')
      expect(validator.isAwaiting(TOPIC, 0)).toBe(false)
    })

    it.each([
      [-1, 5],
      [4, -1],
      [-1, -1],
    ])(
      'an unknown tracked epoch (%i) or leader epoch (%i) validates without a request',
      (trackedEpoch, currentLeaderEpoch) => {
        const validator = create()

        expect(
          validator.request(TOPIC, 0, { offset: '10', trackedEpoch, currentLeaderEpoch }).action
        ).toEqual('validated')
        expect(validator.isAwaiting(TOPIC, 0)).toBe(false)
        // Recorded immediately, so the partition is not re-examined - and re-logged - every round.
        expect(validator.validatedAgainst(TOPIC, 0)).toEqual(currentLeaderEpoch)
      }
    )

    it('answers validated while disabled', () => {
      const validator = create()
      validator.disable()

      expect(
        validator.request(TOPIC, 0, { offset: '10', trackedEpoch: 4, currentLeaderEpoch: 5 }).action
      ).toEqual('validated')
      expect(validator.isDisabled()).toBe(true)
    })
  })

  describe('complete', () => {
    /**
     * Puts the partition into `awaiting` and returns both the validator and the generation
     * `complete()` has to be given back. It returns the token rather than writing a describe-scoped
     * `let`, so no case can read a generation a *previous* case left behind.
     */
    const awaiting = (validator, offset = '10', trackedEpoch = 4) => {
      const { generation } = validator.request(TOPIC, 0, {
        offset,
        trackedEpoch,
        currentLeaderEpoch: 5,
      })
      return { validator, generation }
    }

    it('validates when the end offset is at or beyond the position', () => {
      const { validator, generation } = awaiting(create())

      expect(
        validator.complete(TOPIC, 0, position('10', 4), {
          errorCode: 0,
          endOffset: '10',
          leaderEpoch: 4,
          seekQueued: false,
          generation,
        })
      ).toEqual({ action: 'validated', divergingOffset: null, leaderEpoch: 4 })
      expect(validator.isAwaiting(TOPIC, 0)).toBe(false)
      expect(validator.validatedAgainst(TOPIC, 0)).toEqual(5)
    })

    it('truncates when the end offset is below the position', () => {
      const { validator, generation } = awaiting(create(), '120')

      expect(
        validator.complete(TOPIC, 0, position('120', 4), {
          errorCode: 0,
          endOffset: '100',
          leaderEpoch: 4,
          seekQueued: false,
          generation,
        })
      ).toEqual({ action: 'truncated', divergingOffset: '100', leaderEpoch: 4 })
      expect(validator.isAwaiting(TOPIC, 0)).toBe(false)
    })

    it.each([
      ['string offset -1', '-1', 4],
      ['number epoch -1', '100', -1],
      // The decoded int64 may reach us as a string, a number or a Long: none of them may turn the
      // reset into a truncation to offset -1.
      ['number offset -1', -1, 4],
      ['Long offset -1', Long.fromValue(-1), 4],
      ['string epoch -1', '100', '-1'],
    ])(
      'resets when the end offset or leader epoch is undefined (%s)',
      (_, endOffset, leaderEpoch) => {
        const { validator, generation } = awaiting(create())

        expect(
          validator.complete(TOPIC, 0, position('10', 4), {
            errorCode: 0,
            endOffset,
            leaderEpoch,
            seekQueued: false,
            generation,
          })
        ).toEqual({ action: 'reset', divergingOffset: null, leaderEpoch: -1 })
      }
    )

    it.each([[6], [5], [3], [9], [56], [74], [75], [78]])(
      'answers retry for the retriable code %i, leaving the partition awaiting',
      (errorCode) => {
        const { validator, generation } = awaiting(create())

        expect(
          validator.complete(TOPIC, 0, position('10', 4), {
            errorCode,
            endOffset: '-1',
            leaderEpoch: -1,
            seekQueued: false,
            generation,
          })
        ).toEqual({ action: 'retry', divergingOffset: null, leaderEpoch: -1 })
        expect(validator.isAwaiting(TOPIC, 0)).toBe(true)
      }
    )

    it('answers fatal for a non-retriable code', () => {
      const { validator, generation } = awaiting(create())

      expect(
        validator.complete(TOPIC, 0, position('10', 4), {
          errorCode: 29,
          endOffset: '-1',
          leaderEpoch: -1,
          seekQueued: false,
          generation,
        })
      ).toEqual({ action: 'fatal', divergingOffset: null, leaderEpoch: -1 })
    })

    describe('discards, each returning the partition to unvalidated', () => {
      it('when the partition is no longer awaiting', () => {
        const validator = create()
        // The generation a request would observe right now, so only the awaiting guard can fire.
        const { generation } = validator.request('another-topic', 0, {
          offset: '10',
          trackedEpoch: 4,
          currentLeaderEpoch: 5,
        })

        expect(
          validator.complete(TOPIC, 0, position('10', 4), {
            errorCode: 0,
            endOffset: '10',
            leaderEpoch: 4,
            seekQueued: false,
            generation,
          })
        ).toEqual({ action: 'discarded', divergingOffset: null, leaderEpoch: -1 })
      })

      it('when the position moved under the request', () => {
        const { validator, generation } = awaiting(create(), '10', 4)

        expect(
          validator.complete(TOPIC, 0, position('55', 4), {
            errorCode: 0,
            endOffset: '10',
            leaderEpoch: 4,
            seekQueued: false,
            generation,
          }).action
        ).toEqual('discarded')
        expect(validator.isAwaiting(TOPIC, 0)).toBe(false)
        expect(validator.validatedAgainst(TOPIC, 0)).toBeUndefined()
      })

      it('when the tracked epoch moved under the request', () => {
        const { validator, generation } = awaiting(create(), '10', 4)

        expect(
          validator.complete(TOPIC, 0, position('10', 6), {
            errorCode: 0,
            endOffset: '10',
            leaderEpoch: 4,
            seekQueued: false,
            generation,
          }).action
        ).toEqual('discarded')
      })

      it('when a user seek is queued meanwhile', () => {
        const { validator, generation } = awaiting(create())

        expect(
          validator.complete(TOPIC, 0, position('10', 4), {
            errorCode: 0,
            endOffset: '5',
            leaderEpoch: 4,
            seekQueued: true,
            generation,
          }).action
        ).toEqual('discarded')
      })

      it('when the assignment generation is stale and the key is gone', () => {
        const validator = create()
        const { generation } = validator.request(TOPIC, 0, {
          offset: '10',
          trackedEpoch: 4,
          currentLeaderEpoch: 5,
        })
        validator.clear()

        expect(
          validator.complete(TOPIC, 0, position('10', 4), {
            errorCode: 0,
            endOffset: '10',
            leaderEpoch: 4,
            seekQueued: false,
            generation,
          }).action
        ).toEqual('discarded')
        // A stale reply may not resurrect an entry the new assignment might not own.
        expect(validator.validatedAgainst(TOPIC, 0)).toBeUndefined()
      })

      it('when the generation is stale even though the same key is awaiting again', () => {
        // The case key-absence alone does not cover, and the reason the generation is threaded:
        // after a rebalance the new assignment re-requests the same partition at the same
        // position, and the previous assignment's reply arrives afterwards. Without the
        // generation it would match on { offset, trackedEpoch } and be applied.
        const validator = create()
        const { generation: stale } = validator.request(TOPIC, 0, {
          offset: '10',
          trackedEpoch: 4,
          currentLeaderEpoch: 5,
        })

        validator.clear()
        const { generation: current } = validator.request(TOPIC, 0, {
          offset: '10',
          trackedEpoch: 4,
          currentLeaderEpoch: 5,
        })

        expect(stale).not.toEqual(current)
        expect(
          validator.complete(TOPIC, 0, position('10', 4), {
            errorCode: 0,
            endOffset: '5', // would have truncated the partition, had it been applied
            leaderEpoch: 4,
            seekQueued: false,
            generation: stale,
          }).action
        ).toEqual('discarded')

        // The partition of the *current* assignment is untouched: still awaiting its own reply.
        expect(validator.isAwaiting(TOPIC, 0)).toBe(true)
      })
    })

    describe('no partition is left awaiting once its request is answered', () => {
      const reply = (overrides) => ({
        errorCode: 0,
        endOffset: '10',
        leaderEpoch: 4,
        seekQueued: false,
        ...overrides,
      })

      it.each([
        ['validated', {}, position('10', 4)],
        ['truncated', { endOffset: '5' }, position('10', 4)],
        ['reset', { endOffset: '-1' }, position('10', 4)],
        ['discarded', {}, position('55', 4)],
      ])('%s is an exit complete performs itself', (action, overrides, currentPosition) => {
        const { validator, generation } = awaiting(create())

        expect(
          validator.complete(TOPIC, 0, currentPosition, reply({ ...overrides, generation })).action
        ).toEqual(action)
        expect(validator.isAwaiting(TOPIC, 0)).toBe(false)
      })

      it.each([
        ['retry', { errorCode: 6 }],
        ['fatal', { errorCode: 29 }],
      ])(
        '%s keeps the request in flight, and every caller-side exit ends it',
        (action, overrides) => {
          const exits = [
            (validator) => validator.markValidated(TOPIC, 0, 5),
            (validator) => validator.clear(),
            (validator) => validator.forgetTopic(TOPIC, [0]),
          ]

          for (const exit of exits) {
            const { validator, generation } = awaiting(create())

            expect(
              validator.complete(TOPIC, 0, position('10', 4), reply({ ...overrides, generation }))
                .action
            ).toEqual(action)
            expect(validator.isAwaiting(TOPIC, 0)).toBe(true)

            exit(validator)

            expect(validator.isAwaiting(TOPIC, 0)).toBe(false)
          }
        }
      )
    })
  })

  describe('needsValidation', () => {
    it('is true for a partition that was never validated', () => {
      expect(create().needsValidation(TOPIC, 0, 5)).toBe(true)
    })

    it('is true when the leader epoch moved since the last validation', () => {
      const validator = create()
      validator.markValidated(TOPIC, 0, 4)

      expect(validator.needsValidation(TOPIC, 0, 5)).toBe(true)
    })

    it('is false once validated against that epoch, which is the steady state of a round', () => {
      const validator = create()
      validator.markValidated(TOPIC, 0, 5)

      expect(validator.needsValidation(TOPIC, 0, 5)).toBe(false)
    })

    it('is false while a request is in flight: whoever put it there owns its exit', () => {
      const validator = create()
      validator.request(TOPIC, 0, { offset: '10', trackedEpoch: 4, currentLeaderEpoch: 5 })

      expect(validator.needsValidation(TOPIC, 0, 5)).toBe(false)
      expect(validator.needsValidation(TOPIC, 0, 9)).toBe(false)
    })

    it('agrees with request(): it is false exactly when request answers unchanged', () => {
      const validator = create()
      validator.markValidated(TOPIC, 0, 5)

      expect(validator.needsValidation(TOPIC, 0, 5)).toBe(false)
      expect(
        validator.request(TOPIC, 0, { offset: '10', trackedEpoch: 4, currentLeaderEpoch: 5 }).action
      ).toEqual('unchanged')
    })
  })

  describe('lifetime', () => {
    it('clear drops every key, stamps a new generation and re-arms validation', () => {
      const validator = create()
      const observe = () =>
        validator.request('generation-probe', 0, {
          offset: '10',
          trackedEpoch: 4,
          currentLeaderEpoch: 5,
        }).generation

      validator.markValidated(TOPIC, 0, 5)
      validator.disable()
      const before = observe()

      validator.clear()

      expect(validator.validatedAgainst(TOPIC, 0)).toBeUndefined()
      expect(validator.isDisabled()).toBe(false)
      expect(observe()).not.toEqual(before)
    })

    it('forgetTopic drops exactly that topic partitions', () => {
      const validator = create()
      validator.markValidated(TOPIC, 0, 5)
      validator.markValidated(TOPIC, 1, 5)
      validator.markValidated('other-topic', 0, 5)

      validator.forgetTopic(TOPIC, [0, 1])

      expect(validator.validatedAgainst(TOPIC, 0)).toBeUndefined()
      expect(validator.validatedAgainst(TOPIC, 1)).toBeUndefined()
      expect(validator.validatedAgainst('other-topic', 0)).toEqual(5)
    })

    it('forgetTopic tolerates a topic with no known partitions', () => {
      const validator = create()
      validator.markValidated('other-topic', 0, 5)

      expect(() => validator.forgetTopic(TOPIC)).not.toThrow()
      expect(validator.validatedAgainst('other-topic', 0)).toEqual(5)
    })

    it('markValidated from a previous assignment writes nothing', () => {
      const validator = create()
      // Fetcher A's round puts the partition into validation...
      const { generation } = validator.request(TOPIC, 0, {
        offset: '10',
        trackedEpoch: 4,
        currentLeaderEpoch: 5,
      })

      // ...a rebalance drops it, and fetcher B validates it against the new leader epoch.
      validator.clear()
      validator.request(TOPIC, 0, { offset: '10', trackedEpoch: 4, currentLeaderEpoch: 9 })

      // A's sweep, retry exhaustion or markAllValidated now runs with the epoch it observed.
      expect(validator.markValidated(TOPIC, 0, 5, generation)).toBe(false)
      expect(validator.isAwaiting(TOPIC, 0)).toBe(true)
      expect(validator.validatedAgainst(TOPIC, 0)).toBeUndefined()

      expect(validator.markValidated(TOPIC, 0, 9)).toBe(true)
      expect(validator.validatedAgainst(TOPIC, 0)).toEqual(9)
    })

    it('every mutation is idempotent per key, so two fetchers cannot corrupt one entry', () => {
      const validator = create()

      validator.request(TOPIC, 0, { offset: '10', trackedEpoch: 4, currentLeaderEpoch: 5 })
      const { generation } = validator.request(TOPIC, 0, {
        offset: '10',
        trackedEpoch: 4,
        currentLeaderEpoch: 5,
      })
      expect(validator.isAwaiting(TOPIC, 0)).toBe(true)

      const first = validator.complete(TOPIC, 0, position('10', 4), {
        errorCode: 0,
        endOffset: '10',
        leaderEpoch: 4,
        seekQueued: false,
        generation,
      })
      const duplicate = validator.complete(TOPIC, 0, position('10', 4), {
        errorCode: 0,
        endOffset: '10',
        leaderEpoch: 4,
        seekQueued: false,
        generation,
      })

      expect(first.action).toEqual('validated')
      expect(duplicate.action).toEqual('discarded')
      expect(validator.validatedAgainst(TOPIC, 0)).toEqual(5)
    })
  })

  describe('release', () => {
    const awaiting = (validator) => {
      validator.request(TOPIC, 0, { offset: '10', trackedEpoch: 4, currentLeaderEpoch: 5 })
      return { topic: TOPIC, partition: 0, entry: validator.entryOf(TOPIC, 0) }
    }

    it('returns its own pending entry to unvalidated', () => {
      const validator = create()
      validator.release([awaiting(validator)])
      expect(validator.isAwaiting(TOPIC, 0)).toBe(false)
      expect(validator.needsValidation(TOPIC, 0, 5)).toBe(true)
    })

    it('leaves a replacement or an already validated entry alone', () => {
      const validator = create()
      const stale = awaiting(validator)
      validator.forgetTopic(TOPIC, [0])
      const replacement = awaiting(validator)
      validator.release([stale])
      expect(validator.isAwaiting(TOPIC, 0)).toBe(true)
      validator.markValidated(TOPIC, 0, 5)
      validator.release([replacement])
      expect(validator.validatedAgainst(TOPIC, 0)).toBe(5)
    })
  })
})
