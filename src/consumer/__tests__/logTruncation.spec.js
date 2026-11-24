const apiKeys = require('../../protocol/requests/apiKeys')
const {
  events: { LOG_TRUNCATION },
} = require('../instrumentationEvents')
const {
  NODE_ID,
  TOPIC,
  prepareRecordBatches,
  fetchResponse,
  offsetForLeaderEpochResponse,
  createFetchGroup,
} = require('./fixtures/fetchHarness')

beforeAll(prepareRecordBatches)

/**
 * Named states of one partition's validation round, so a case can say which outcome it is about
 * rather than restating the option pairs that produce it.
 */
const SCENARIOS = {
  validated: { position: '10', endOffset: '20' },
  truncated: { position: '120', endOffset: '100' },
  reset: { position: '120', endOffset: '-1' },
  discarded: { position: '120', endOffset: '100', seekDuringValidation: true },
  'retriable-exhausted': { validationErrorCode: 6 },
  'non-retriable': { validationErrorCode: -1 }, // UNKNOWN_SERVER_ERROR; 29 fails the round instead
  'transport-failure': { validationThrows: true },
  'unsupported-api': { advertiseOffsetForLeaderEpoch: false },
}

/**
 * The shared fetch harness with the defaults every case here shares: a committed epoch and a
 * cached leader epoch that differ, which is exactly the state that makes the first round of an
 * assignment validate.
 *
 * @param {object} [options] the harness options, plus `validationScenario` (a key of `SCENARIOS`)
 *   and `seekDuringValidation` (queue a user seek while the reply is being written)
 * @returns {Promise<object>} the harness result
 */
const createGroup = async ({ validationScenario, ...overrides } = {}) => {
  const { seekDuringValidation = false, ...options } = {
    committedEpoch: 4,
    leaderEpoch: 5,
    position: '10',
    endOffset: '20',
    ...SCENARIOS[validationScenario],
    ...overrides,
  }

  const harness = await createFetchGroup(options)

  if (seekDuringValidation) {
    harness.setValidationResponse((partitions) => {
      harness.group.seek({ topic: TOPIC, partition: 0, offset: '500' })
      return offsetForLeaderEpochResponse(
        partitions.map((partition) => ({ partition, endOffset: options.endOffset }))
      )
    })
  }

  return harness
}

/** The api keys of every frame written, in order. */
const apiKeysOf = (requests) => requests().map(({ apiKey }) => apiKey)

/** How many OffsetForLeaderEpoch frames were written. */
const validationCount = (requests) =>
  apiKeysOf(requests).filter((apiKey) => apiKey === apiKeys.OffsetForLeaderEpoch).length

/** The partitions one decoded Fetch frame asks for. */
const partitionsOf = (frame) =>
  frame.topics.flatMap(({ partitions }) => partitions.map(({ partition }) => partition))

/** The message every warning about a held partition carries. */
const HELD_WARNING =
  'Position validation did not complete; the partition is not fetched until it validates'

/**
 * A monotonic clock the case moves by hand: `performance.now()` answers `clock.now`, which is what
 * the per-partition backoff and the warning interval read. Timers stay real, so the in-round
 * retrier keeps its own short delays. Restored by the describe's `afterEach`.
 * @returns {{ now: number }}
 */
const manualClock = () => {
  const clock = { now: 1000 }
  jest.spyOn(performance, 'now').mockImplementation(() => clock.now)
  return clock
}

describe('Consumer > position validation', () => {
  test('validation runs before the first fetch of an assignment, and goes to the leader', async () => {
    // The partition is read from a preferred replica (node 2) while its leader is node 0.
    const { group, requests, findBrokerCalls } = await createGroup({ preferredReadReplica: 2 })

    // Validation drops the replica preference, so node 2 is owed nothing; the leader's round then
    // fetches, and only after the validation.
    await expect(group.fetch(2)).resolves.toEqual([])
    await group.fetch(NODE_ID)

    const order = apiKeysOf(requests)
    expect(order.indexOf(apiKeys.Fetch)).toBeGreaterThan(-1)
    expect(order.indexOf(apiKeys.OffsetForLeaderEpoch)).toBeLessThan(order.indexOf(apiKeys.Fetch))
    expect(findBrokerCalls).toContainEqual(expect.objectContaining({ nodeId: 0 })) // the leader, not the replica
  })

  test('validatePositions never returns with a partition still awaiting', async () => {
    for (const scenario of Object.keys(SCENARIOS)) {
      const { group } = await createGroup({ validationScenario: scenario })

      await group.validatePositions([{ topic: TOPIC, partitions: [0] }])

      expect(group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
    }
  })

  test('a partition whose reply never arrives does not stay awaiting either', async () => {
    const { group } = await createGroup({ partitions: [0, 1], withholdValidationFor: [0] })

    await group.validatePositions([{ topic: TOPIC, partitions: [0, 1] }])

    expect(group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
    expect(group.positionValidator.isAwaiting(TOPIC, 1)).toBe(false)
  })

  test('a leader that does not advertise OffsetForLeaderEpoch is never asked', async () => {
    const { group, cluster, requests, logger } = await createGroup({
      validationScenario: 'unsupported-api',
    })
    const info = jest.spyOn(logger, 'info')

    const batches = await group.fetch(NODE_ID)

    expect(validationCount(requests)).toEqual(0)
    expect(info).toHaveBeenCalledTimes(1)
    // "the partitions are fetched normally" has to be asserted as delivery, not as "an array came
    // back": ConsumerGroup#fetch always returns an array, so toBeDefined() cannot fail.
    expect(batches).toHaveLength(1)

    // The leader epoch moves, so the second round really re-enters validation and reaches the
    // broker check again - without this the memo would never be consulted twice.
    cluster.leaderEpoch = () => 6
    await group.fetch(NODE_ID)

    expect(info).toHaveBeenCalledTimes(1) // one notice per broker per assignment, not per round
    expect(validationCount(requests)).toEqual(0)
  })

  test('with an unknown tracked epoch, a leader-epoch change still clears the preferred read replica', async () => {
    // No request is made, but the partition still counts as having changed leader.
    const { group } = await createGroup({ preferredReadReplica: 2, committedEpoch: -1 })

    await group.validatePositions([{ topic: TOPIC, partitions: [0] }])

    expect(group.preferredReadReplicasPerTopicPartition[TOPIC][0]).toBeUndefined()
  })

  test('entering validation clears the partition preferred read replica', async () => {
    const { group } = await createGroup({ preferredReadReplica: 2 })

    // The round runs against node 2, which is where the replica preference routed the partition.
    await group.fetch(2)

    expect(group.preferredReadReplicasPerTopicPartition[TOPIC][0]).toBeUndefined()
  })

  test('nothing thrown inside validation reaches the fetch, and the partition is held', async () => {
    const { group, cluster, logger, requests, requestRejoin } = await createGroup()
    const warn = jest.spyOn(logger, 'warn')
    cluster.findLeaderForPartitions = () => {
      throw new Error('metadata gone')
    }

    await expect(group.fetch(NODE_ID)).resolves.toEqual([])
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledWith(
      HELD_WARNING,
      expect.objectContaining({ topic: TOPIC, partition: 0, error: 'metadata gone' })
    )
    expect(apiKeysOf(requests)).not.toContain(apiKeys.Fetch)
    expect(group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
    expect(group.positionValidator.holdsFetch(TOPIC, 0)).toBe(true)
    expect(requestRejoin).not.toHaveBeenCalled()
  })

  test('an unexpected failure inside validation is warned once, never thrown, asked once, and holds the partition', async () => {
    const { group, logger, requests, requestRejoin, setValidationResponse } = await createGroup()
    const warn = jest.spyOn(logger, 'warn')
    setValidationResponse(() => {
      throw new TypeError('answer is not a function')
    })

    await expect(group.fetch(NODE_ID)).resolves.toEqual([])
    expect(warn).toHaveBeenCalledTimes(1)
    // An unrecoverable error is not retried, so the round costs exactly one attempt.
    expect(validationCount(requests)).toEqual(1)
    expect(apiKeysOf(requests)).not.toContain(apiKeys.Fetch)
    expect(group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
    expect(group.positionValidator.holdsFetch(TOPIC, 0)).toBe(true)
    expect(requestRejoin).not.toHaveBeenCalled()
  })

  test('a rebalance during a failing validation leaves the tracked epoch alone', async () => {
    const { group, offsetManager, setValidationResponse } = await createGroup()
    setValidationResponse(() => {
      group.positionValidator.clear() // the group re-synced while the request was in flight
      throw new Error('connection lost')
    })

    await group.validatePositions([{ topic: TOPIC, partitions: [0] }])

    // The partition belongs to a new assignment now: giving up on the old request must not clear
    // an epoch, nor mark validated, on that assignment's behalf.
    expect(offsetManager.lastConsumedEpoch(TOPIC, 0)).toEqual(4)
    expect(group.positionValidator.validatedAgainst(TOPIC, 0)).toBeUndefined()
    expect(group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
  })

  test('a discarded validation reply does not strand the partition', async () => {
    const { group, setResponse } = await createGroup({ validationScenario: 'discarded' })

    await group.validatePositions([{ topic: TOPIC, partitions: [0] }])
    expect(group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
    expect(group.positionValidator.validatedAgainst(TOPIC, 0)).toBeUndefined()

    setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: true }]))
    expect(await group.fetch(NODE_ID)).toHaveLength(1)
  })

  describe('outcomes', () => {
    test('validated: the position stands', async () => {
      const { group, offsetManager } = await createGroup({ validationScenario: 'validated' })
      const before = offsetManager.nextOffset(TOPIC, 0).toString()

      await group.fetch(NODE_ID)

      expect(offsetManager.nextOffset(TOPIC, 0).toString()).toEqual(before)
      expect(group.positionValidator.validatedAgainst(TOPIC, 0)).toEqual(5)
    })

    test('truncated: seek to the diverging offset, warn, and emit the event', async () => {
      const events = []
      const { group, logger, offsetManager } = await createGroup({
        validationScenario: 'truncated',
      })
      const warn = jest.spyOn(logger, 'warn')
      group.instrumentationEmitter.addListener(LOG_TRUNCATION, (e) => events.push(e.payload))

      await group.fetch(NODE_ID)

      expect(events).toEqual([
        { topic: TOPIC, partition: 0, offset: '120', divergingOffset: '100', leaderEpoch: 4 },
      ])
      expect(offsetManager.nextOffset(TOPIC, 0).toString()).toEqual('100')
      expect(warn).toHaveBeenCalledTimes(1)
    })

    test('undefined: reset per fromBeginning and emit the event with a null diverging offset', async () => {
      const events = []
      const { group, offsetManager } = await createGroup({ validationScenario: 'reset' })
      const setDefaultOffset = jest.spyOn(offsetManager, 'setDefaultOffset')
      group.instrumentationEmitter.addListener(LOG_TRUNCATION, (e) => events.push(e.payload))

      await group.fetch(NODE_ID)

      expect(setDefaultOffset).toHaveBeenCalledWith(
        { topic: TOPIC, partition: 0 },
        expect.objectContaining({ guard: expect.any(Function) })
      )
      expect(events).toEqual([
        { topic: TOPIC, partition: 0, offset: '120', divergingOffset: null, leaderEpoch: -1 },
      ])
    })

    test('retriable exhausted: the epoch is kept, the offset is not moved, the partition is not fetched until it validates', async () => {
      const { group, offsetManager, fetchFrames } = await createGroup({
        validationScenario: 'retriable-exhausted',
        position: '120',
      })

      const batches = await group.fetch(NODE_ID)

      expect(batches).toEqual([])
      expect(fetchFrames()).toEqual([])
      expect(offsetManager.lastConsumedEpoch(TOPIC, 0)).toEqual(4)
      expect(offsetManager.positionOf(TOPIC, 0)).toEqual('120')
      expect(group.positionValidator.holdsFetch(TOPIC, 0)).toBe(true)
    })
  })

  test('a truncation repositions the partition in the same round', async () => {
    const { group, fetchRequestOffsets } = await createGroup({
      validationScenario: 'truncated',
      autoCommit: false,
    })

    await group.fetch(NODE_ID)

    // autoCommit: false sets resolvedOffsets directly, so the request built immediately
    // afterwards already carries the corrected offset.
    expect(fetchRequestOffsets()).toEqual(['100'])
  })

  test('with autoCommit the truncated partition is dropped from this round and resumes from the corrected offset', async () => {
    const {
      group,
      broker,
      fetchRequestPartitions,
      fetchRequestOffsets,
      setResponse,
      setValidationResponse,
    } = await createGroup({ position: '120', autoCommit: true, partitions: [0, 1] })

    // Only partition 0 diverges; partition 1's epoch still ends past its position.
    setValidationResponse((partitions) =>
      offsetForLeaderEpochResponse(
        partitions.map((partition) => ({ partition, endOffset: partition === 0 ? '100' : '200' }))
      )
    )
    setResponse(fetchResponse([{ partition: 1, errorCode: 0 }]))

    await group.fetch(NODE_ID)
    expect(fetchRequestPartitions()).toEqual([1]) // the committed-offset filter dropped it

    jest.spyOn(broker, 'offsetFetch').mockResolvedValue({
      responses: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '100', metadata: '' }] }],
    })
    setResponse(
      fetchResponse([
        { partition: 0, errorCode: 0 },
        { partition: 1, errorCode: 0 },
      ])
    )

    await group.fetch(NODE_ID)
    expect(fetchRequestOffsets()).toContain('100')
  })

  test('a queued seek wins over the validation outcome, and a partition carrying one is not validated', async () => {
    const { group, requests, offsetManager } = await createGroup({
      validationScenario: 'truncated',
    })
    group.seek({ topic: TOPIC, partition: 0, offset: '500' })

    await group.fetch(NODE_ID)

    expect(validationCount(requests)).toEqual(0)
    expect(offsetManager.nextOffset(TOPIC, 0).toString()).toEqual('500')
  })

  test('a seek queued while validation was awaiting keeps the partition out of the next scan', async () => {
    // The narrow window the exclusion exists for: `seekOffsets()` popped this round's seeks
    // already, so the only entry the scan can find is one queued after that.
    const { group, requests } = await createGroup({ validationScenario: 'truncated' })
    group.seek({ topic: TOPIC, partition: 0, offset: '500' })

    await group.validatePositions([{ topic: TOPIC, partitions: [0] }])

    expect(validationCount(requests)).toEqual(0)
    expect(group.positionValidator.validatedAgainst(TOPIC, 0)).toBeUndefined()
  })

  test('a paused partition is validated, may be repositioned, and stays paused', async () => {
    const { group, offsetManager } = await createGroup({ validationScenario: 'truncated' })
    group.pause([{ topic: TOPIC, partitions: [0] }])

    await group.fetch(NODE_ID)

    expect(offsetManager.nextOffset(TOPIC, 0).toString()).toEqual('100')
    expect(group.isPaused(TOPIC, 0)).toBe(true)
  })

  test('the validation retrier is bounded and does not spend the user retry budget', async () => {
    const { group, requests } = await createGroup({
      validationScenario: 'retriable-exhausted',
      retry: { retries: 50 },
    })

    await group.fetch(NODE_ID)

    // createRetry({ retries: 2 }) means three attempts, not fifty-one; the partition is then held.
    expect(validationCount(requests)).toEqual(3)
    expect(apiKeysOf(requests)).not.toContain(apiKeys.Fetch)
  })

  test('a retry is narrowed to the partitions that asked for one', async () => {
    const { group, validationRequests, setValidationResponse } = await createGroup({
      partitions: [0, 1],
    })

    setValidationResponse((partitions) =>
      offsetForLeaderEpochResponse(
        partitions.map((partition) => ({ partition, errorCode: partition === 0 ? 6 : 0 }))
      )
    )

    await group.fetch(NODE_ID)

    expect(
      validationRequests().map(({ topics }) =>
        topics[0].partitions.map(({ partition }) => partition)
      )
    ).toEqual([[0, 1], [0], [0]])
  })

  test('a partition another fetcher is validating is not in this round request', async () => {
    // Two fetchers can compute the leader map at different moments and both claim a partition, so
    // a partition can be awaiting a reply this fetch round never asked for. That is the state
    // stubbed here: a single-fetcher round cannot produce it, because the gate blocks first.
    const { group, fetchRequestPartitions } = await createGroup({ partitions: [0, 1] })
    jest
      .spyOn(group.positionValidator, 'holdsFetch')
      .mockImplementation((topic, partition) => partition === 0)

    await group.fetch(NODE_ID)

    expect(fetchRequestPartitions()).toEqual([1])
  })

  test('a fetched partition that entered validation meanwhile is dropped, not delivered', async () => {
    const { group, setResponse } = await createGroup({ committedEpoch: -1 })
    let awaiting = false
    jest.spyOn(group.positionValidator, 'holdsFetch').mockImplementation(() => awaiting)

    // The body is written after the request went out, which is the only window in which a
    // partition can enter validation between its own request and its own response.
    setResponse(() => {
      awaiting = true
      return fetchResponse([{ partition: 0, errorCode: 0, records: true }])
    })

    expect(await group.fetch(NODE_ID)).toHaveLength(0)
  })

  test('a rebalance leaves every partition of the new assignment unvalidated', async () => {
    const { group, sync } = await createGroup()
    await group.fetch(NODE_ID)
    expect(group.positionValidator.validatedAgainst(TOPIC, 0)).toEqual(5)

    await sync()

    expect(group.positionValidator.validatedAgainst(TOPIC, 0)).toBeUndefined()
    expect(group.consecutiveFetchErrors.size).toEqual(0)
    expect(group.validationUnusableLogged.size).toEqual(0)
  })

  test('a rebalance while a reply is in flight makes that reply a no-op', async () => {
    const { group, offsetManager, setValidationResponse } = await createGroup({
      validationScenario: 'truncated',
    })

    setValidationResponse((partitions) => {
      group.positionValidator.clear() // what applyRebalanceResult does when the group re-syncs
      return offsetForLeaderEpochResponse(
        partitions.map((partition) => ({ partition, endOffset: '100' }))
      )
    })

    await group.validatePositions([{ topic: TOPIC, partitions: [0] }])

    expect(offsetManager.nextOffset(TOPIC, 0).toString()).toEqual('120')
    expect(group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
    expect(group.positionValidator.validatedAgainst(TOPIC, 0)).toBeUndefined()
  })

  test('a validation entry does not survive a topic invalidation', async () => {
    const { group } = await createGroup()
    await group.fetch(NODE_ID)

    group.invalidateTopic(TOPIC)

    expect(group.positionValidator.validatedAgainst(TOPIC, 0)).toBeUndefined()
  })

  test('an unknown tracked epoch or an unknown leader epoch sends no request', async () => {
    for (const state of [
      { committedEpoch: -1, leaderEpoch: 5 },
      { committedEpoch: 4, leaderEpoch: -1 },
    ]) {
      const { group, requests } = await createGroup(state)

      await group.fetch(NODE_ID)

      expect(validationCount(requests)).toEqual(0)
      expect(group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
    }
  })

  describe('a partition with no committed offset', () => {
    /** The resolved offsets of the topic, including any key holding no value. */
    const resolvedKeys = (offsetManager) => Object.keys(offsetManager.resolvedOffsets[TOPIC])

    test('is not read by validation, so nothing it touches can hold an offset that is not one', async () => {
      const { group, offsetManager } = await createGroup()
      // What a concurrent OffsetOutOfRange recovery does to a partition mid-round.
      offsetManager.clearOffsets({ topic: TOPIC, partition: 0 })
      const nextOffset = jest.spyOn(offsetManager, 'nextOffset')

      await group.validatePositions([{ topic: TOPIC, partitions: [0] }])

      expect(nextOffset).not.toHaveBeenCalled()
      expect(resolvedKeys(offsetManager)).toEqual([])
      expect(() => offsetManager.countResolvedOffsets()).not.toThrow()
      expect(() => offsetManager.uncommittedOffsets()).not.toThrow()
    })

    test('is not read when it loses its offset while its validation reply is in flight', async () => {
      const harness = await createGroup()
      const nextOffset = jest.spyOn(harness.offsetManager, 'nextOffset')
      let readsBeforeTheClear

      harness.setValidationResponse((partitions) => {
        harness.offsetManager.clearOffsets({ topic: TOPIC, partition: 0 })
        readsBeforeTheClear = nextOffset.mock.calls.length
        return offsetForLeaderEpochResponse(
          partitions.map((partition) => ({ partition, endOffset: '20' }))
        )
      })

      await harness.group.fetch(NODE_ID)

      // The reply lands after the clear, and applying it must not read the position back.
      expect(nextOffset.mock.calls.length).toEqual(readsBeforeTheClear)
      expect(resolvedKeys(harness.offsetManager)).toEqual([])
      // Both walk the resolved offsets, and both are on the auto-commit path.
      expect(() => harness.offsetManager.countResolvedOffsets()).not.toThrow()
      expect(() => harness.offsetManager.uncommittedOffsets()).not.toThrow()
    })
  })

  test('a round that finishes after a rebalance leaves the new assignment alone', async () => {
    const harness = await createGroup()
    const validator = harness.group.positionValidator

    harness.setValidationResponse((partitions) => {
      // The rebalance runs while this request is in flight, and the fetcher that owns the
      // partition in the new assignment puts it back into validation against the new epoch.
      validator.clear()
      validator.request(TOPIC, 0, { offset: '10', trackedEpoch: 4, currentLeaderEpoch: 9 })
      return offsetForLeaderEpochResponse(
        partitions.map((partition) => ({ partition, endOffset: '20' }))
      )
    })

    await harness.group.fetch(NODE_ID)

    // The old round's sweep must not answer for the new one: its epoch was never checked.
    expect(validator.isAwaiting(TOPIC, 0)).toBe(true)
    expect(validator.validatedAgainst(TOPIC, 0)).toBeUndefined()
  })

  test('a round whose partitions are already validated reads no offset and asks nothing', async () => {
    const { group, offsetManager } = await createGroup()
    await group.fetch(NODE_ID)

    const request = jest.spyOn(group.positionValidator, 'request')
    const nextOffset = jest.spyOn(offsetManager, 'nextOffset')

    await group.validatePositions([{ topic: TOPIC, partitions: [0] }])

    expect(request).not.toHaveBeenCalled()
    expect(nextOffset).not.toHaveBeenCalled()
  })

  describe('a validation that does not complete', () => {
    afterEach(() => jest.restoreAllMocks())

    /** Partition 1 has no tracked epoch, so it is fetched every round without a request. */
    const withBusySibling = async (options) => {
      const harness = await createGroup({ partitions: [0, 1], ...options })
      harness.offsetManager.resolvedEpochs[TOPIC][1] = -1
      return harness
    }

    /** Answers `errorCode` for partition 0 and a valid end offset for any other. */
    const failPartitionZero = (errorCode) => (partitions) =>
      offsetForLeaderEpochResponse(
        partitions.map((partition) =>
          partition === 0
            ? { partition, errorCode, leaderEpoch: -1, endOffset: '-1' }
            : { partition, endOffset: '20' }
        )
      )

    test.each([false, true])(
      'retriable answers until the budget is spent: the partition is not fetched, keeps offset 100 and epoch 1, and a later round validates and resumes at the divergence 50 (autoCommit %s)',
      async (autoCommit) => {
        const clock = manualClock()
        const harness = await createGroup({
          committedEpoch: 1,
          leaderEpoch: 2,
          position: '100',
          autoCommit,
        })
        const { group, broker, offsetManager, offsetCommit, fetchFrames } = harness
        let answer = { errorCode: 56, leaderEpoch: -1, endOffset: '-1' } // KAFKA_STORAGE_ERROR
        harness.setValidationResponse((partitions) =>
          offsetForLeaderEpochResponse(partitions.map((partition) => ({ partition, ...answer })))
        )

        // The budget is spent in the first round; the second, at the same epoch, is still held.
        await expect(group.fetch(NODE_ID)).resolves.toEqual([])
        await expect(group.fetch(NODE_ID)).resolves.toEqual([])

        expect(harness.validationRequests()).toHaveLength(3)
        expect(fetchFrames()).toEqual([])
        expect(offsetCommit).not.toHaveBeenCalled()
        expect(offsetManager.positionOf(TOPIC, 0)).toEqual('100')
        expect(offsetManager.lastConsumedEpoch(TOPIC, 0)).toEqual(1)

        // The new leader's log ends epoch 1 at 50: records 50-99 of epoch 2 must not be skipped.
        answer = { errorCode: 0, leaderEpoch: 1, endOffset: '50' }
        clock.now += 100
        await group.fetch(NODE_ID)
        expect(harness.validationRequests()).toHaveLength(4)

        if (autoCommit) {
          // The divergence is committed and read back before the partition is fetched again.
          const committed = offsetCommit.mock.calls.flatMap(([{ topics }]) =>
            topics.flatMap(({ partitions }) => partitions.map(({ offset }) => offset))
          )
          expect(committed).toEqual(['50'])
          jest.spyOn(broker, 'offsetFetch').mockResolvedValue({
            responses: [
              { topic: TOPIC, partitions: [{ partition: 0, offset: '50', metadata: '' }] },
            ],
          })
          await group.fetch(NODE_ID)
        } else {
          expect(offsetCommit).not.toHaveBeenCalled()
        }

        expect(fetchFrames().map(partitionsOf)).toEqual([[0]])
        expect(harness.fetchRequestOffsets()).toEqual(['50'])
      }
    )

    test('a transport failure of the validation request keeps the partition out of the next two Fetch requests and forces a metadata refresh', async () => {
      manualClock()
      const { group, cluster, fetchFrames, validationRequests } = await withBusySibling({
        validationScenario: 'transport-failure',
      })

      await group.fetch(NODE_ID)
      await group.fetch(NODE_ID)

      expect(fetchFrames().map(partitionsOf)).toEqual([[1], [1]])
      // The in-round budget (three attempts) is spent in the first round only.
      expect(validationRequests()).toHaveLength(3)
      // Asked for once, by the round after the failure, before that round's validation.
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    })

    test('a reposition that fails keeps the partition out of the Fetch in this and the next round, and a later round applies it', async () => {
      const clock = manualClock()
      const events = []
      const { group, offsetManager, fetchFrames, fetchRequestOffsets } = await withBusySibling({
        validationScenario: 'truncated',
      })
      group.instrumentationEmitter.addListener(LOG_TRUNCATION, (e) => events.push(e.payload))
      jest
        .spyOn(offsetManager, 'reposition')
        .mockRejectedValueOnce(new Error('coordinator is down'))

      await group.fetch(NODE_ID)
      await group.fetch(NODE_ID)

      // The outcome was decided but never applied, so nothing may claim it was.
      expect(events).toEqual([])
      expect(offsetManager.positionOf(TOPIC, 0)).toEqual('120')
      expect(fetchFrames().map(partitionsOf)).toEqual([[1], [1]])

      clock.now += 100
      await group.fetch(NODE_ID)

      expect(events).toEqual([
        { topic: TOPIC, partition: 0, offset: '120', divergingOffset: '100', leaderEpoch: 4 },
      ])
      expect(fetchFrames().map(partitionsOf)).toEqual([[1], [1], [0, 1]])
      expect(fetchRequestOffsets()).toEqual(['100', '120'])
    })

    test('a truncation outcome keeps the partition out of the Fetch until the reposition has been applied', async () => {
      const { group, offsetManager, fetchRequestOffsets } = await withBusySibling({
        validationScenario: 'truncated',
      })
      const reposition = offsetManager.reposition.bind(offsetManager)
      let builtDuringReposition
      jest.spyOn(offsetManager, 'reposition').mockImplementation(async (...args) => {
        // What a Fetch built while the reposition's own request is in flight would ask for.
        builtDuringReposition = group.buildFetchRequests([{ topic: TOPIC, partitions: [0, 1] }])
        return reposition(...args)
      })

      await group.fetch(NODE_ID)

      expect(builtDuringReposition.flatMap(({ partitions }) => partitions)).toEqual([
        expect.objectContaining({ partition: 1 }),
      ])
      expect(fetchRequestOffsets()).toEqual(['100', '120'])
    })

    test('leader move: the old leader answers NOT_LEADER_OR_FOLLOWER, the forced refresh names the new leader, which validates; the partition is fetched within the backoff, not after metadataMaxAge', async () => {
      manualClock()
      const harness = await createGroup({ knownNodes: [0, 1] })
      const { group, cluster, fetchFrames, validationRequests, installRouting } = harness
      let answered = 0
      harness.setValidationResponse((partitions) =>
        offsetForLeaderEpochResponse(
          partitions.map((partition) =>
            ++answered <= 3
              ? { partition, errorCode: 6, leaderEpoch: -1, endOffset: '-1' }
              : { partition, endOffset: '20' }
          )
        )
      )
      cluster.refreshMetadata.mockImplementation(async () => {
        installRouting({ 0: 1 })
        cluster.leaderEpoch = () => 6
      })

      await expect(group.fetch(0)).resolves.toEqual([])
      expect(fetchFrames()).toEqual([])

      // The clock never moves: neither the backoff nor metadataMaxAge (300000 ms) is waited for.
      const batches = await group.fetch(1)

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
      expect(
        validationRequests().map(({ topics }) => topics[0].partitions[0].currentLeaderEpoch)
      ).toEqual([5, 5, 5, 6])
      expect(fetchFrames().map(partitionsOf)).toEqual([[0]])
      expect(harness.lastFetchRequest()[0].currentLeaderEpoch).toEqual(6)
      expect(batches.map(({ partition }) => partition)).toEqual([0])
    })

    test('UNKNOWN_LEADER_EPOCH forgets the cached epoch, is not retried in the same round, and the refreshed epoch validates', async () => {
      const harness = await createGroup()
      const { group, cluster, fetchFrames, validationRequests } = harness
      let errorCode = 75
      harness.setValidationResponse((partitions) =>
        offsetForLeaderEpochResponse(
          partitions.map((partition) =>
            errorCode
              ? { partition, errorCode, leaderEpoch: -1, endOffset: '-1' }
              : { partition, endOffset: '20' }
          )
        )
      )

      await expect(group.fetch(NODE_ID)).resolves.toEqual([])
      expect(validationRequests()).toHaveLength(1)
      expect(cluster.forgetLeaderEpoch).toHaveBeenCalledWith(TOPIC, 0)
      expect(fetchFrames()).toEqual([])

      cluster.refreshMetadata.mockImplementation(async () => {
        cluster.leaderEpoch = () => 6
      })
      errorCode = 0
      await group.fetch(NODE_ID)

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
      expect(
        validationRequests().map(({ topics }) => topics[0].partitions[0].currentLeaderEpoch)
      ).toEqual([5, 6])
      expect(fetchFrames().map(partitionsOf)).toEqual([[0]])
    })

    test('per-partition backoff: with a busy sibling partition, a failing partition is asked again no sooner than 100 ms, then 200 ms, … at most 1000 ms; a new epoch is asked at once', async () => {
      const clock = manualClock()
      const harness = await withBusySibling()
      const { group, cluster, fetchFrames, validationRequests } = harness
      harness.setValidationResponse(failPartitionZero(-1))
      const roundAt = async (at) => {
        clock.now = 1000 + at
        await group.fetch(NODE_ID)
        return validationRequests().length
      }

      const asked = []
      for (const at of [0, 99, 100, 299, 300, 699, 700, 1499, 1500, 2499, 2500]) {
        asked.push(await roundAt(at))
      }

      // 100, 200, 400, 800, then capped at 1000 ms after each failure.
      expect(asked).toEqual([1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6])
      cluster.leaderEpoch = () => 6
      expect(await roundAt(2500)).toEqual(7)
      // The sibling is fetched every round; the failing partition never is.
      expect(fetchFrames().map(partitionsOf)).toEqual(Array(12).fill([1]))
    })

    test('an unexpected non-retriable answer holds that partition and keeps validating and fetching the others', async () => {
      const harness = await createGroup({ partitions: [0, 1] })
      const { group, cluster, fetchFrames, validationRequests } = harness
      harness.setValidationResponse(failPartitionZero(-1)) // UNKNOWN_SERVER_ERROR

      await group.fetch(NODE_ID)

      expect(group.positionValidator.validatedAgainst(TOPIC, 1)).toEqual(5)
      expect(fetchFrames().map(partitionsOf)).toEqual([[1]])

      // A new leader epoch is validated for both: validation was not switched off.
      cluster.leaderEpoch = () => 6
      await group.fetch(NODE_ID)

      expect(
        validationRequests().map(({ topics }) =>
          topics[0].partitions.map(({ partition }) => partition)
        )
      ).toEqual([
        [0, 1],
        [0, 1],
      ])
      expect(group.positionValidator.validatedAgainst(TOPIC, 1)).toEqual(6)
      expect(fetchFrames().map(partitionsOf)).toEqual([[1], [1]])
    })

    test('TOPIC_AUTHORIZATION_FAILED on a validation answer fails the fetch round with that error; a sibling in the same answer is still validated', async () => {
      const harness = await createGroup({ partitions: [0, 1] })
      const { group, fetchFrames, requestRejoin } = harness
      harness.setValidationResponse(failPartitionZero(29))

      await expect(group.fetch(NODE_ID)).rejects.toMatchObject({
        type: 'TOPIC_AUTHORIZATION_FAILED',
        code: 29,
      })

      expect(group.positionValidator.validatedAgainst(TOPIC, 1)).toEqual(5)
      expect(group.positionValidator.holdsFetch(TOPIC, 0)).toBe(true)
      expect(fetchFrames()).toEqual([])
      expect(requestRejoin).not.toHaveBeenCalled()
    })

    test('one answer with 29 for p0 and a retriable code for p1: p1 is retried in the round and validates on the next attempt, then the round fails with the 29 error, and p0 stays out of the next Fetch', async () => {
      manualClock()
      const harness = await createGroup({ partitions: [0, 1] })
      const { group, fetchFrames, validationRequests } = harness
      let attempt = 0
      harness.setValidationResponse((partitions) => {
        attempt += 1
        return offsetForLeaderEpochResponse(
          partitions.map((partition) => {
            if (partition === 0) {
              return { partition, errorCode: 29, leaderEpoch: -1, endOffset: '-1' }
            }
            return attempt === 1
              ? { partition, errorCode: 6, leaderEpoch: -1, endOffset: '-1' }
              : { partition, endOffset: '20' }
          })
        )
      })

      await expect(group.fetch(NODE_ID)).rejects.toMatchObject({ code: 29 })

      expect(
        validationRequests().map(({ topics }) =>
          topics[0].partitions.map(({ partition }) => partition)
        )
      ).toEqual([[0, 1], [1]])
      expect(group.positionValidator.validatedAgainst(TOPIC, 1)).toEqual(5)

      // The clock has not moved, so partition 0 is still within its backoff.
      await group.fetch(NODE_ID)

      expect(validationRequests()).toHaveLength(2)
      expect(fetchFrames().map(partitionsOf)).toEqual([[1]])
    })

    test('an OFFSET_OUT_OF_RANGE answer to a Fetch built before the partition became held arrives while it is held: no reset, position and committed offset unchanged', async () => {
      const harness = await createGroup()
      const { group, cluster, offsetManager, fetchFrames } = harness
      // Validated against the current epoch, so the round's Fetch goes out at once.
      group.positionValidator.markValidated(TOPIC, 0, 5)
      harness.setValidationResponse(failPartitionZero(-1))
      const setDefaultOffset = jest.spyOn(offsetManager, 'setDefaultOffset').mockResolvedValue()
      let validating

      // Written while the Fetch is out: the epoch moves and validation of partition 0 begins.
      harness.setResponse(() => {
        cluster.leaderEpoch = () => 6
        validating = group.validatePositions([{ topic: TOPIC, partitions: [0] }])
        return fetchResponse([{ partition: 0, errorCode: 1 }])
      })

      await expect(group.fetch(NODE_ID)).resolves.toEqual([])
      await validating

      expect(fetchFrames().map(partitionsOf)).toEqual([[0]])
      expect(setDefaultOffset).not.toHaveBeenCalled()
      expect(group.consecutiveFetchErrors.size).toEqual(0)
      expect(offsetManager.positionOf(TOPIC, 0)).toEqual('10')
      expect(offsetManager.committedOffsets()[TOPIC][0]).toEqual('10')
      expect(group.positionValidator.holdsFetch(TOPIC, 0)).toBe(true)
    })

    test('after a slow in-round retry (fake time advanced past the backoff during the attempts), the same epoch is not asked again for 100 ms after exhaustion; a new epoch is asked at once; the next warning comes only after 60 s', async () => {
      const clock = manualClock()
      const harness = await createGroup()
      const { group, cluster, logger, validationRequests } = harness
      const warn = jest.spyOn(logger, 'warn')
      const heldWarnings = () => warn.mock.calls.filter(([message]) => message === HELD_WARNING)
      // Every attempt takes 400 ms of the monotonic clock, so three take 1200 ms.
      harness.setValidationResponse((partitions) => {
        clock.now += 400
        return offsetForLeaderEpochResponse(
          partitions.map((partition) => ({
            partition,
            errorCode: 6,
            leaderEpoch: -1,
            endOffset: '-1',
          }))
        )
      })

      await group.fetch(NODE_ID) // exhausted at 2200, so held until 2300
      expect(validationRequests()).toHaveLength(3)
      expect(heldWarnings()).toHaveLength(1)

      await group.fetch(NODE_ID)
      clock.now = 2299
      await group.fetch(NODE_ID)
      expect(validationRequests()).toHaveLength(3)

      clock.now = 2300
      await group.fetch(NODE_ID) // exhausted at 3500, held until 3700
      expect(validationRequests()).toHaveLength(6)

      cluster.leaderEpoch = () => 6
      await group.fetch(NODE_ID) // a new epoch is asked within the backoff
      expect(validationRequests()).toHaveLength(9)
      expect(heldWarnings()).toHaveLength(1)

      clock.now = 2200 + 60000
      await group.fetch(NODE_ID)
      expect(validationRequests()).toHaveLength(12)
      expect(heldWarnings()).toHaveLength(2)
      expect(heldWarnings()[1][1]).toEqual(expect.objectContaining({ partition: 0, failures: 4 }))
    })

    test('the first failure warns once with topic, partition, error and failures; a second failure within a minute does not warn; one after a minute does', async () => {
      const clock = manualClock()
      const harness = await createGroup()
      const { group, logger } = harness
      harness.setValidationResponse(failPartitionZero(-1))
      const warn = jest.spyOn(logger, 'warn')

      await group.fetch(NODE_ID)
      clock.now += 100
      await group.fetch(NODE_ID)
      clock.now = 1000 + 60000
      await group.fetch(NODE_ID)

      const error = 'The server experienced an unexpected error when processing the request'
      expect(warn.mock.calls).toEqual([
        [
          HELD_WARNING,
          {
            groupId: 'test-group',
            memberId: group.memberId,
            topic: TOPIC,
            partition: 0,
            error,
            failures: 1,
            heldMs: 0,
          },
        ],
        [
          HELD_WARNING,
          {
            groupId: 'test-group',
            memberId: group.memberId,
            topic: TOPIC,
            partition: 0,
            error,
            failures: 3,
            heldMs: 60000,
          },
        ],
      ])
    })

    test('another partition of the same node is fetched while one is held', async () => {
      const { group, setResponse, setValidationResponse, fetchFrames } = await withBusySibling({
        position: '0', // the shared record batch holds offset 0
      })
      setValidationResponse(failPartitionZero(-1))
      setResponse(fetchResponse([{ partition: 1, errorCode: 0, records: true }]))

      const batches = await group.fetch(NODE_ID)

      expect(fetchFrames().map(partitionsOf)[0]).toContain(1)
      expect(batches.filter(({ partition }) => partition === 1)).toHaveLength(1)
      expect(batches.find(({ partition }) => partition === 1).messages).toHaveLength(1)
    })

    test('a broker with no usable OffsetForLeaderEpoch version is fetched unvalidated with one notice', async () => {
      const { group, logger, requests } = await createGroup({
        validationScenario: 'unsupported-api',
      })
      const info = jest.spyOn(logger, 'info')
      const warn = jest.spyOn(logger, 'warn')

      const batches = await group.fetch(NODE_ID)

      expect(batches).toHaveLength(1)
      expect(validationCount(requests)).toEqual(0)
      expect(info).toHaveBeenCalledTimes(1)
      expect(warn).not.toHaveBeenCalled()
      expect(group.positionValidator.validatedAgainst(TOPIC, 0)).toEqual(5)
    })

    test('a seek of a held partition releases it at the sought position', async () => {
      const { group, fetchFrames, fetchRequestOffsets } = await createGroup({
        validationScenario: 'non-retriable',
      })

      await group.fetch(NODE_ID)
      group.seek({ topic: TOPIC, partition: 0, offset: '500' })
      await group.fetch(NODE_ID)

      expect(fetchFrames().map(partitionsOf).at(-1)).toEqual([0])
      expect(fetchRequestOffsets()).toEqual(['500'])
    })
  })
})
