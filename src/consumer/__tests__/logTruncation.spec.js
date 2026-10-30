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
  'non-retriable': { validationErrorCode: 29 },
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

  test('while validation is disabled, a leader-epoch change still clears the preferred read replica', async () => {
    const { group } = await createGroup({ preferredReadReplica: 2 })
    group.positionValidator.disable()

    await group.validatePositions([{ topic: TOPIC, partitions: [0] }])

    expect(group.preferredReadReplicasPerTopicPartition[TOPIC][0]).toBeUndefined()
  })

  test('entering validation clears the partition preferred read replica', async () => {
    const { group } = await createGroup({ preferredReadReplica: 2 })

    // The round runs against node 2, which is where the replica preference routed the partition.
    await group.fetch(2)

    expect(group.preferredReadReplicasPerTopicPartition[TOPIC][0]).toBeUndefined()
  })

  test('a non-retriable validation error disables validation for the assignment, and sync re-arms it', async () => {
    // Two partitions on two different leaders, both read from node 0 through a replica preference,
    // so one round asks two brokers: the first answers non-retriable and the second must never be
    // asked at all.
    const { group, cluster, logger, requests, setResponse, sync } = await createGroup({
      validationScenario: 'non-retriable',
      partitions: [0, 1],
      partitionLeaders: { 0: 0, 1: 1 },
      preferredReadReplica: NODE_ID,
    })
    const warn = jest.spyOn(logger, 'warn')
    // The round fetches partition 0 alone, so the broker answers for that partition only.
    setResponse(fetchResponse([{ partition: 0, errorCode: 0 }]))

    await group.fetch(NODE_ID)

    expect(warn).toHaveBeenCalledTimes(1)
    expect(validationCount(requests)).toEqual(1)
    expect(group.positionValidator.isDisabled()).toBe(true)

    // Partition 1 went back to its own leader when validation dropped its replica preference, so
    // this round is node 0's partition alone.
    cluster.leaderEpoch = () => 6
    setResponse(fetchResponse([{ partition: 0, errorCode: 0 }]))
    await group.fetch(NODE_ID)

    expect(warn).toHaveBeenCalledTimes(1) // one warn per assignment, not per round
    expect(validationCount(requests)).toEqual(1)

    await sync()
    expect(group.positionValidator.isDisabled()).toBe(false)
  })

  test('nothing thrown inside validation reaches the fetch', async () => {
    const { group, cluster, logger, requests, requestRejoin } = await createGroup()
    const warn = jest.spyOn(logger, 'warn')
    cluster.findLeaderForPartitions = () => {
      throw new Error('metadata gone')
    }

    await expect(group.fetch(NODE_ID)).resolves.toHaveLength(1)
    expect(warn).toHaveBeenCalledTimes(1)
    expect(apiKeysOf(requests)).toContain(apiKeys.Fetch)
    expect(group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
    expect(requestRejoin).not.toHaveBeenCalled()
  })

  test('a reposition that fails leaves the partition unvalidated, and the next round applies it', async () => {
    const events = []
    const { group, offsetManager } = await createGroup({ validationScenario: 'truncated' })
    group.instrumentationEmitter.addListener(LOG_TRUNCATION, (e) => events.push(e.payload))
    jest.spyOn(offsetManager, 'seek').mockRejectedValueOnce(new Error('coordinator is down'))

    await group.fetch(NODE_ID)

    // The outcome was decided but never applied, so nothing may claim it was.
    expect(events).toEqual([])
    expect(offsetManager.nextOffset(TOPIC, 0).toString()).toEqual('120')
    expect(group.positionValidator.validatedAgainst(TOPIC, 0)).toBeUndefined()

    await group.fetch(NODE_ID)

    expect(events).toEqual([
      { topic: TOPIC, partition: 0, offset: '120', divergingOffset: '100', leaderEpoch: 4 },
    ])
    expect(offsetManager.nextOffset(TOPIC, 0).toString()).toEqual('100')
  })

  test('an unexpected failure inside validation is warned, never thrown, and asked once', async () => {
    const { group, logger, requests, requestRejoin, setValidationResponse } = await createGroup()
    const warn = jest.spyOn(logger, 'warn')
    setValidationResponse(() => {
      throw new TypeError('answer is not a function')
    })

    await expect(group.fetch(NODE_ID)).resolves.toHaveLength(1)
    expect(warn).toHaveBeenCalledTimes(1)
    // An unrecoverable error is not retried, so the round costs exactly one attempt.
    expect(validationCount(requests)).toEqual(1)
    expect(group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
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

    test('retriable exhausted: the epoch is cleared, the offset is not moved, the partition is fetched', async () => {
      const { group, offsetManager } = await createGroup({
        validationScenario: 'retriable-exhausted',
        position: '120',
      })

      const batches = await group.fetch(NODE_ID)

      expect(offsetManager.lastConsumedEpoch(TOPIC, 0)).toEqual(-1)
      expect(offsetManager.nextOffset(TOPIC, 0).toString()).toEqual('120')
      expect(batches).toHaveLength(1)
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

    // createRetry({ retries: 2 }) means three attempts, not fifty-one.
    expect(validationCount(requests)).toEqual(3)
    expect(apiKeysOf(requests)).toContain(apiKeys.Fetch)
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
      .spyOn(group.positionValidator, 'isAwaiting')
      .mockImplementation((topic, partition) => partition === 0)

    await group.fetch(NODE_ID)

    expect(fetchRequestPartitions()).toEqual([1])
  })

  test('a fetched partition that entered validation meanwhile is dropped, not delivered', async () => {
    const { group, setResponse } = await createGroup({ committedEpoch: -1 })
    let awaiting = false
    jest.spyOn(group.positionValidator, 'isAwaiting').mockImplementation(() => awaiting)

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
})
