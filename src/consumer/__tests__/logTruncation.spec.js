const apiKeys = require('../../protocol/requests/apiKeys')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const Batch = require('../batch')
const {
  events: { LOG_TRUNCATION },
} = require('../instrumentationEvents')
const {
  NODE_ID,
  TOPIC,
  TOPIC_ID,
  prepareRecordBatches,
  buildRecordBatch,
  fetchResponse,
  offsetForLeaderEpochResponse,
  scriptedEpochLog,
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
    jest.spyOn(offsetManager, 'reposition').mockRejectedValueOnce(new Error('coordinator is down'))

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

    test('truncated: a reposition that a newer seek superseded emits no LOG_TRUNCATION and marks nothing changed, while one that applies emits exactly one', async () => {
      const events = []
      const { group, offsetManager, offsetCommit } = await createGroup({
        validationScenario: 'truncated',
        autoCommit: true,
        partitions: [0, 1],
      })
      group.instrumentationEmitter.addListener(LOG_TRUNCATION, (e) => events.push(e.payload))
      const markChanged = jest.spyOn(group, 'markChanged')
      const reposition = jest.spyOn(offsetManager, 'reposition')
      // Partition 0's commit is answered after a seek to the earliest offset was applied to the
      // same partition. That newer operation supersedes the reposition, which resolves false.
      offsetCommit.mockImplementationOnce(async () => {
        await offsetManager.seek({ topic: TOPIC, partition: 0, offset: '-2' })
        return { topics: [] }
      })

      await group.validatePositions([{ topic: TOPIC, partitions: [0, 1] }])

      expect(reposition.mock.calls.map(([{ partition }]) => partition)).toEqual([0, 1])
      await expect(reposition.mock.results[0].value).resolves.toBe(false)
      await expect(reposition.mock.results[1].value).resolves.toBe(true)
      expect(events).toEqual([
        { topic: TOPIC, partition: 1, offset: '120', divergingOffset: '100', leaderEpoch: 4 },
      ])
      expect(markChanged.mock.calls).toEqual([[TOPIC, 1]])
    })

    test('truncated: a reposition superseded by a newer publication at the same position still emits LOG_TRUNCATION and marks the partition changed', async () => {
      const events = []
      const { group, offsetManager, offsetCommit } = await createGroup({
        validationScenario: 'truncated',
        autoCommit: true,
      })
      group.instrumentationEmitter.addListener(LOG_TRUNCATION, (e) => events.push(e.payload))
      const markChanged = jest.spyOn(group, 'markChanged')
      const reposition = jest.spyOn(offsetManager, 'reposition')
      // While the rewind's commit is held, another participant of the group (a transactional
      // producer) begins and confirms a newer offset publication for the same cell at 120, the
      // position the truncation was detected at. It supersedes the rewind without moving it.
      offsetCommit.mockImplementationOnce(async () => {
        const ledger = offsetManager.operations
        const ticket = ledger.begin([{ topic: TOPIC, partition: 0 }], { mutation: true })
        ledger.publish(ticket, TOPIC, 0, '120')
        ledger.apply(ticket, TOPIC, 0)
        ledger.end(ticket)
        return { topics: [] }
      })

      await group.validatePositions([{ topic: TOPIC, partitions: [0] }])

      await expect(reposition.mock.results[0].value).resolves.toBe(false)
      expect(offsetManager.positionOf(TOPIC, 0)).toEqual('120')
      expect(events).toEqual([
        { topic: TOPIC, partition: 0, offset: '120', divergingOffset: '100', leaderEpoch: 4 },
      ])
      expect(markChanged.mock.calls).toEqual([[TOPIC, 0]])
    })

    test('truncated: a newer publication of the same offset in another spelling still emits LOG_TRUNCATION and marks the partition changed', async () => {
      const events = []
      const { group, offsetManager, offsetCommit } = await createGroup({
        validationScenario: 'truncated',
        autoCommit: true,
      })
      group.instrumentationEmitter.addListener(LOG_TRUNCATION, (e) => events.push(e.payload))
      const markChanged = jest.spyOn(group, 'markChanged')
      const reposition = jest.spyOn(offsetManager, 'reposition')
      offsetCommit.mockImplementationOnce(async () => {
        const ledger = offsetManager.operations
        const ticket = ledger.begin([{ topic: TOPIC, partition: 0 }], { mutation: true })
        ledger.publish(ticket, TOPIC, 0, '0120')
        ledger.apply(ticket, TOPIC, 0)
        ledger.end(ticket)
        return { topics: [] }
      })

      await group.validatePositions([{ topic: TOPIC, partitions: [0] }])

      await expect(reposition.mock.results[0].value).resolves.toBe(false)
      expect(BigInt(offsetManager.positionOf(TOPIC, 0))).toEqual(120n)
      expect(events).toEqual([
        { topic: TOPIC, partition: 0, offset: '120', divergingOffset: '100', leaderEpoch: 4 },
      ])
      expect(markChanged.mock.calls).toEqual([[TOPIC, 0]])
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
    // a partition can be awaiting a reply this fetch round never asked for. That is the state put
    // in place here: a single-fetcher round cannot produce it, because the gate blocks first.
    const { group, setResponse, fetchRequestPartitions } = await createGroup({
      partitions: [0, 1],
    })
    group.positionValidator.request(TOPIC, 0, {
      offset: '10',
      trackedEpoch: 4,
      currentLeaderEpoch: 5,
    })
    // A broker answers only the partitions a full request carried.
    setResponse(fetchResponse([{ partition: 1, errorCode: 0 }]))

    await group.fetch(NODE_ID)

    expect(group.positionValidator.isAwaiting(TOPIC, 0)).toBe(true)
    expect(fetchRequestPartitions()).toEqual([1])
  })

  test('a fetched partition that entered validation meanwhile is dropped, not delivered', async () => {
    const { group, setResponse } = await createGroup({ committedEpoch: -1 })

    // The body is written after the request went out, which is the only window in which a
    // partition can enter validation between its own request and its own response.
    setResponse(() => {
      group.positionValidator.request(TOPIC, 0, {
        offset: '10',
        trackedEpoch: 4,
        currentLeaderEpoch: 6,
      })
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

describe('Consumer > answers from before a leader epoch change', () => {
  /** One partition-0 row per effect an answer can have besides a batch. */
  const ROWS = {
    records: { partition: 0, errorCode: 0, records: true },
    'offset out of range': { partition: 0, errorCode: 1 },
  }
  const SESSIONS = [
    ['off', false],
    ['on', true],
  ]
  /** Partition 1 shares the node and its epoch never moves: it is delivered as before. */
  const HEALTHY = { partition: 1, errorCode: 0, records: true }
  /** Partition 1 with nothing new. */
  const IDLE = { partition: 1, errorCode: 0 }
  /** The id a broker gives a new Fetch session. */
  const SESSION_ID = 77

  /** @returns {number[]} the partitions of the batches one round returned */
  const partitionsOf = (batches) => batches.map(({ partition }) => partition)

  /** @returns {[number, string[]][]} the partitions and offsets one round handed out records for */
  const deliveredOf = (batches) =>
    batches
      .filter((batch) => !batch.isEmpty())
      .map(({ partition, messages }) => [partition, messages.map(({ offset }) => offset)])

  /**
   * @param {{topics: {partitions: object[]}[]}} frame a decoded Fetch frame
   * @returns {[number, string, number][]} its partitions with fetch offset and current epoch, in
   *   partition order: a sessionless request lists them in a random order, as kafkajs 2.2.4 did
   */
  const cursorsOf = (frame) =>
    frame.topics
      .flatMap(({ partitions }) => partitions)
      .map(({ partition, fetchOffset, currentLeaderEpoch }) => [
        partition,
        fetchOffset,
        currentLeaderEpoch,
      ])
      .sort(([a], [b]) => a - b)

  /** Lets queued work run until `condition` holds, or fails the test. */
  const until = async (condition) => {
    for (let turns = 0; turns < 500 && !condition(); turns++) {
      await new Promise(setImmediate)
    }
    if (!condition()) throw new Error('the schedule never reached its hold point')
  }

  /**
   * Two partitions on node 0 at cached epoch 5, read live from the metadata cache so a hint moves
   * the epoch the group sees. A reset is recorded and never committed.
   * @param {object} [options] harness overrides
   * @returns {Promise<object>} the harness, plus `reset`, the `setDefaultOffset` spy
   */
  const createGroup = async (options) => {
    const harness = await createFetchGroup({ partitions: [0, 1], liveRouting: true, ...options })
    const reset = jest.spyOn(harness.offsetManager, 'setDefaultOffset').mockResolvedValue()
    return { ...harness, reset }
  }

  /**
   * The first round of a group: a full request, answered with nothing new and, with sessions on,
   * a session id, so the next request is incremental.
   * @param {object} h the harness
   * @param {boolean} fetchSessions
   * @returns {Promise<void>}
   */
  const openRound = async (h, fetchSessions) => {
    h.setResponse(
      fetchResponse([{ partition: 0, errorCode: 0 }, IDLE], {
        sessionId: fetchSessions ? SESSION_ID : 0,
      })
    )
    expect(deliveredOf(await h.group.fetch(NODE_ID))).toEqual([])
  }

  test.each(Object.keys(ROWS).flatMap((name) => SESSIONS.map(([label, on]) => [name, label, on])))(
    '%s: an answer to a Fetch built at epoch 5 that arrives after metadata moved to 6 makes no batch, no reset and no leader hint, with fetch sessions %s',
    async (name, _label, fetchSessions) => {
      const { group, cluster, metadataCache, reset, setResponse, lastFetchRequest } =
        await createGroup({ fetchSessions })
      setResponse(() => {
        // The answer reaches the client after the cache learned epoch 6 for partition 0 alone.
        metadataCache.applyLeaderHint(TOPIC, 0, { leaderId: NODE_ID, leaderEpoch: 6 })
        return fetchResponse([ROWS[name], HEALTHY])
      })

      const batches = await group.fetch(NODE_ID)

      // A sessionless request lists its partitions in a random order, as kafkajs 2.2.4 did.
      expect(
        lastFetchRequest()
          .map(({ partition, currentLeaderEpoch }) => [partition, currentLeaderEpoch])
          .sort(([a], [b]) => a - b)
      ).toEqual([
        [0, 5],
        [1, 5],
      ])
      expect(partitionsOf(batches)).toEqual([1])
      expect(reset).not.toHaveBeenCalled()
      expect(cluster.applyLeaderHint).not.toHaveBeenCalled()
    }
  )

  test.each([['records'], ['offset out of range']])(
    '%s: an answer built before the partition was validated again at the same epoch and offset makes no batch and no reset',
    async (name) => {
      const { group, offsetManager, reset, setResponse, validationRequests } = await createGroup({
        committedEpoch: 4,
        position: '120',
        endOffset: '500',
      })
      // Withheld while partition 0 leaves validation and is validated again at the same epoch and
      // position: the same verdict, under a new validation entry.
      setResponse(() =>
        (async () => {
          group.positionValidator.forgetTopic(TOPIC, [0])
          await group.validatePositions([{ topic: TOPIC, partitions: [0] }])
          return fetchResponse([ROWS[name], HEALTHY])
        })()
      )

      const batches = await group.fetch(NODE_ID)

      expect(validationRequests()).toHaveLength(2)
      expect(group.positionValidator.validatedAgainst(TOPIC, 0)).toBe(5)
      expect(offsetManager.positionOf(TOPIC, 0)).toBe('120')
      expect(partitionsOf(batches)).toEqual([1])
      expect(reset).not.toHaveBeenCalled()
    }
  )

  /** Partition 1 is led by node 1, so node 0's request carries partition 0 alone. */
  const splitLeaders = { knownNodes: [NODE_ID, 1], partitionLeaders: { 0: NODE_ID, 1: 1 } }
  /** An answer that also names partition 1, which node 0's request did not carry. */
  const answerNamingPartition1 = (sessionId = 0) =>
    fetchResponse(
      [
        { partition: 1, errorCode: 1 },
        { partition: 0, errorCode: 0, records: true },
      ],
      { sessionId }
    )

  test('a sessionless answer naming a partition the request did not carry fails the round before any row is used', async () => {
    const { group, reset, setResponse, fetchRequestPartitions } = await createGroup(splitLeaders)
    setResponse(answerNamingPartition1())

    await expect(group.fetch(NODE_ID)).rejects.toThrow(
      'Fetch response names partition 1 of topic test-topic that the request did not carry'
    )
    expect(fetchRequestPartitions()).toEqual([0])
    expect(reset).not.toHaveBeenCalled()
  })

  test('an incremental session answer naming a partition the request did not carry is refused whole before any row is used', async () => {
    const { group, reset, setResponse, fetchFrames } = await createGroup({
      ...splitLeaders,
      fetchSessions: true,
    })
    setResponse(fetchResponse([{ partition: 0, errorCode: 0 }], { sessionId: SESSION_ID }))
    expect(deliveredOf(await group.fetch(NODE_ID))).toEqual([])
    setResponse(answerNamingPartition1(SESSION_ID))

    await expect(group.fetch(NODE_ID)).resolves.toEqual([])
    expect(fetchFrames().map(({ sessionId, sessionEpoch }) => [sessionId, sessionEpoch])).toEqual([
      [0, 0],
      [SESSION_ID, 1],
    ])
    expect(reset).not.toHaveBeenCalled()
  })

  test.each(SESSIONS)(
    "a partition whose cached epoch moves after the round's validation answered is left out of that round's Fetch frame, with fetch sessions %s",
    async (_label, fetchSessions) => {
      // The leader's state for each check: at epoch 5 for the first, at epoch 6 (entered at its
      // log end) for the second. It answers, then its epoch moves on.
      const brokerAt = {
        5: scriptedEpochLog({
          epochs: [
            [4, '0'],
            [5, '300'],
          ],
          logEnd: '500',
        }),
        6: scriptedEpochLog({
          epochs: [
            [4, '0'],
            [5, '300'],
            [6, '500'],
          ],
          logEnd: '500',
        }),
      }
      const {
        group,
        metadataCache,
        setResponse,
        setValidationResponse,
        fetchRequestPartitions,
        validationRequests,
      } = await createGroup({ fetchSessions, committedEpoch: 4, position: '10' })
      // Every validation answer for partition 0 is overtaken by a newer epoch for it, so even the
      // round's second preparation ends with partition 0 validated against an older epoch.
      setValidationResponse((partitions, request) => {
        const answer = brokerAt[metadataCache.leaderEpoch(TOPIC, 0)].answer(partitions, request)
        if (partitions.includes(0)) {
          const next = metadataCache.leaderEpoch(TOPIC, 0) + 1
          metadataCache.applyLeaderHint(TOPIC, 0, { leaderId: NODE_ID, leaderEpoch: next })
        }
        return answer
      })
      setResponse(fetchResponse([HEALTHY]))

      const batches = await group.fetch(NODE_ID)

      expect(
        validationRequests().map(({ topics }) =>
          topics[0].partitions.map(({ partition, currentLeaderEpoch }) => [
            partition,
            currentLeaderEpoch,
          ])
        )
      ).toEqual([
        [
          [0, 5],
          [1, 5],
        ],
        [[0, 6]],
      ])
      expect(group.positionValidator.validatedAgainst(TOPIC, 0)).toBe(6)
      expect(metadataCache.leaderEpoch(TOPIC, 0)).toBe(7)
      expect(fetchRequestPartitions()).toEqual([1])
      expect(partitionsOf(batches)).toEqual([1])
    }
  )

  describe('the routing a dropped row carries', () => {
    /**
     * Answers node 0's Fetch, built at epoch 5, with `row` for partition 0 and records for
     * partition 1, after the cache learned `cachedEpoch` for partition 0 from elsewhere.
     */
    const answerAfterEpoch = (h, cachedEpoch, row, options) =>
      h.setResponse(() => {
        h.metadataCache.applyLeaderHint(TOPIC, 0, { leaderId: NODE_ID, leaderEpoch: cachedEpoch })
        return fetchResponse([{ partition: 0, ...row }, HEALTHY], options)
      })

    test('a dropped FENCED_LEADER_EPOCH row carrying a leader at epoch 7 moves the cached leader there and makes no batch, reset or error count', async () => {
      const h = await createGroup({ knownNodes: [NODE_ID, 1] })
      // Node 0 fences the request at epoch 5: the partition moved to node 1 at epoch 7.
      answerAfterEpoch(
        h,
        6,
        { errorCode: 74, currentLeader: { leaderId: 1, leaderEpoch: 7 } },
        { nodeEndpoints: [{ nodeId: 1, host: 'kafka1', port: 9092, rack: null }] }
      )

      const batches = await h.group.fetch(NODE_ID)

      expect(h.cluster.applyLeaderHint).toHaveBeenCalledTimes(1)
      expect([h.metadataCache.leader(TOPIC, 0), h.metadataCache.leaderEpoch(TOPIC, 0)]).toEqual([
        1, 7,
      ])
      expect(partitionsOf(batches)).toEqual([1])
      expect(h.reset).not.toHaveBeenCalled()
      expect(h.group.consecutiveFetchErrors.has(`${TOPIC}|0`)).toBe(false)
      expect(h.cluster.refreshMetadata).not.toHaveBeenCalled()
    })

    test('a dropped NOT_LEADER_OR_FOLLOWER row with no usable hint requests the metadata refresh', async () => {
      const h = await createGroup()
      // The broker knows no leader for the partition: its hint names none.
      answerAfterEpoch(h, 6, { errorCode: 6, currentLeader: { leaderId: -1, leaderEpoch: -1 } })

      const batches = await h.group.fetch(NODE_ID)

      expect(h.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
      expect(h.cluster.applyLeaderHint).not.toHaveBeenCalled()
      expect(partitionsOf(batches)).toEqual([1])
      expect(h.reset).not.toHaveBeenCalled()
      expect(h.group.consecutiveFetchErrors.has(`${TOPIC}|0`)).toBe(false)
    })

    test('a dropped row whose hint is older than the cached epoch leaves the cached leader and epoch alone', async () => {
      const h = await createGroup({ knownNodes: [NODE_ID, 1] })
      // The hint names the leader at epoch 6; the cache already holds epoch 7.
      answerAfterEpoch(
        h,
        7,
        { errorCode: 74, currentLeader: { leaderId: 1, leaderEpoch: 6 } },
        { nodeEndpoints: [{ nodeId: 1, host: 'kafka1', port: 9092, rack: null }] }
      )

      const batches = await h.group.fetch(NODE_ID)

      expect(h.cluster.applyLeaderHint).toHaveBeenCalledTimes(1)
      await expect(h.cluster.applyLeaderHint.mock.results[0].value).resolves.toBe(false)
      expect([h.metadataCache.leader(TOPIC, 0), h.metadataCache.leaderEpoch(TOPIC, 0)]).toEqual([
        NODE_ID,
        7,
      ])
      expect(partitionsOf(batches)).toEqual([1])
      expect(h.reset).not.toHaveBeenCalled()
    })

    test('a dropped row of a recreated topic applies no hint', async () => {
      const h = await createGroup({ knownNodes: [NODE_ID, 1] })
      h.setResponse(() => {
        h.metadataCache.applyLeaderHint(TOPIC, 0, { leaderId: NODE_ID, leaderEpoch: 6 })
        // The cache now holds another incarnation of the topic name.
        h.cluster.topicId = () => '11121314-1516-1718-191a-1b1c1d1e1f20'
        return fetchResponse(
          [
            { partition: 0, errorCode: 74, currentLeader: { leaderId: 1, leaderEpoch: 7 } },
            HEALTHY,
          ],
          { nodeEndpoints: [{ nodeId: 1, host: 'kafka1', port: 9092, rack: null }] }
        )
      })

      const batches = await h.group.fetch(NODE_ID)

      expect(h.cluster.applyLeaderHint).not.toHaveBeenCalled()
      expect(h.cluster.refreshMetadata).not.toHaveBeenCalled()
      expect([h.metadataCache.leader(TOPIC, 0), h.metadataCache.leaderEpoch(TOPIC, 0)]).toEqual([
        NODE_ID,
        6,
      ])
      expect(partitionsOf(batches)).toEqual([1])
    })
  })

  describe('replay from the last published offset', () => {
    const SESSION_ROWS = [
      ['sessions off', false],
      ['sessions on', true],
    ]
    /** @returns {object} the answer options for a group with or without a session */
    const answering = (fetchSessions) => ({ sessionId: fetchSessions ? SESSION_ID : 0 })
    /** @returns {[number, string, number]} partition 0's cursor in the last Fetch frame */
    const lastCursorOf0 = (h) => {
      const frames = h.fetchFrames()
      return cursorsOf(frames[frames.length - 1]).find(([partition]) => partition === 0)
    }

    test.each(SESSION_ROWS)(
      '%s: an answer dropped because the leader epoch moved is fetched again from the last published offset and delivered once',
      async (_label, fetchSessions) => {
        const h = await createGroup({ fetchSessions })
        try {
          await openRound(h, fetchSessions)
          // The same leader moves to epoch 6 while the answer, with records for both, is on its way.
          h.setResponse(() => {
            h.metadataCache.applyLeaderHint(TOPIC, 0, { leaderId: NODE_ID, leaderEpoch: 6 })
            return fetchResponse([ROWS.records, HEALTHY], answering(fetchSessions))
          })
          expect(deliveredOf(await h.group.fetch(NODE_ID))).toEqual([[1, ['0']]])

          h.setResponse(fetchResponse([ROWS.records, IDLE], answering(fetchSessions)))
          const replay = await h.group.fetch(NODE_ID)

          const frames = h.fetchFrames()
          expect(frames).toHaveLength(3)
          // With a session both partitions were unchanged, so the dropped answer's request
          // carried them without sending them.
          expect(cursorsOf(frames[1])).toEqual(
            fetchSessions
              ? []
              : [
                  [0, '0', 5],
                  [1, '0', 5],
                ]
          )
          expect(h.offsetManager.positionOf(TOPIC, 0)).toBe('0')
          expect(lastCursorOf0(h)).toEqual([0, '0', 6])
          expect(deliveredOf(replay)).toEqual([[0, ['0']]])
        } finally {
          await h.teardown()
        }
      }
    )

    test.each(SESSION_ROWS)(
      '%s: a queued batch dropped after a leader epoch change is fetched again from the last published offset',
      async (_label, fetchSessions) => {
        const h = await createGroup({ fetchSessions })
        try {
          await openRound(h, fetchSessions)
          h.setResponse(fetchResponse([ROWS.records, IDLE], answering(fetchSessions)))
          const [queued] = (await h.group.fetch(NODE_ID)).filter(({ partition }) => partition === 0)
          expect(deliveredOf([queued])).toEqual([[0, ['0']]])

          // The epoch moves while the batch waits: the validation it was fetched under still
          // stands, but the runner's admission check no longer admits the batch, so nothing is
          // handed over or resolved from it.
          h.metadataCache.applyLeaderHint(TOPIC, 0, { leaderId: NODE_ID, leaderEpoch: 6 })
          const admission = Batch.admissionOf(queued)
          expect(admission.epoch).toBe(5)
          expect(admission.entry).toBe(h.group.positionValidator.entryOf(TOPIC, 0))
          expect(h.group.admissionEntry(TOPIC, 0, admission.epoch)).toBeUndefined()

          h.setResponse(fetchResponse([ROWS.records, IDLE], answering(fetchSessions)))
          const replay = await h.group.fetch(NODE_ID)

          expect(h.offsetManager.positionOf(TOPIC, 0)).toBe('0')
          expect(lastCursorOf0(h)).toEqual([0, '0', 6])
          expect(deliveredOf(replay)).toEqual([[0, ['0']]])
        } finally {
          await h.teardown()
        }
      }
    )

    test.each(SESSION_ROWS)(
      '%s: an eachMessage batch stopped after message 10 resumes at message 11',
      async (_label, fetchSessions) => {
        const h = await createGroup({ fetchSessions, position: '10' })
        // Records 10 to 12 were written at epoch 5; the leader entered epoch 6 at its log end, 13.
        h.setValidationResponse(
          scriptedEpochLog({
            epochs: [
              [5, '0'],
              [6, '13'],
            ],
            logEnd: '13',
          }).answer
        )
        const records = await buildRecordBatch({
          firstOffset: '10',
          lastOffsetDelta: 2,
          partitionLeaderEpoch: 5,
          records: [0, 1, 2].map((offsetDelta) =>
            RecordEncoder({ offsetDelta, key: Buffer.from('k'), value: Buffer.from('v') })
          ),
        })
        const answer = () =>
          fetchResponse([{ partition: 0, errorCode: 0, records }, IDLE], answering(fetchSessions))
        try {
          await openRound(h, fetchSessions)
          h.setResponse(answer())
          expect(deliveredOf(await h.group.fetch(NODE_ID))).toEqual([[0, ['10', '11', '12']]])

          // What the runner does for message 10; the epoch then moves and stops the batch before
          // message 11.
          h.group.resolveOffset({ topic: TOPIC, partition: 0, offset: '10', leaderEpoch: 5 })
          h.metadataCache.applyLeaderHint(TOPIC, 0, { leaderId: NODE_ID, leaderEpoch: 6 })
          h.setResponse(answer())
          const resumed = await h.group.fetch(NODE_ID)

          expect(h.validationRequests().map(({ topics }) => topics[0].partitions)).toEqual([
            [{ partition: 0, currentLeaderEpoch: 6, leaderEpoch: 5 }],
          ])
          expect(lastCursorOf0(h)).toEqual([0, '11', 6])
          expect(deliveredOf(resumed)).toEqual([[0, ['11', '12']]])
        } finally {
          await h.teardown()
        }
      }
    )

    test.each(SESSION_ROWS)(
      '%s: a Fetch whose partition moved to a new leader epoch while the request waited for its broker is not written, and the next frame carries both partitions from their offsets',
      async (_label, fetchSessions) => {
        const h = await createGroup({ fetchSessions })
        try {
          await openRound(h, fetchSessions)
          const lookups = h.findBrokerCalls.length
          const release = h.holdBrokerLookup(NODE_ID)
          h.setResponse(
            fetchResponse([{ partition: 0, errorCode: 0 }, IDLE], answering(fetchSessions))
          )
          const held = h.group.fetch(NODE_ID)
          await until(() => h.findBrokerCalls.length > lookups)

          // Built at epoch 5 (with a session, both partitions unchanged and not sent); the cache
          // learns epoch 6 before the frame reaches the socket.
          h.metadataCache.applyLeaderHint(TOPIC, 0, { leaderId: NODE_ID, leaderEpoch: 6 })
          release()

          await expect(held).resolves.toEqual([])
          expect(h.fetchFrames()).toHaveLength(1)

          h.setResponse(fetchResponse([ROWS.records, HEALTHY], answering(fetchSessions)))
          const next = await h.group.fetch(NODE_ID)

          const frames = h.fetchFrames()
          expect(frames).toHaveLength(2)
          expect(cursorsOf(frames[1])).toEqual([
            [0, '0', 6],
            [1, '0', 5],
          ])
          expect(deliveredOf(next)).toEqual([
            [0, ['0']],
            [1, ['0']],
          ])
        } finally {
          await h.teardown()
        }
      }
    )
  })

  test('a validation answered after the leader moved away and back to the same epoch admits nothing: the partition is checked again before it is fetched', async () => {
    const h = await createGroup({ committedEpoch: 4, position: '10', knownNodes: [NODE_ID, 1] })
    const atEpoch5 = scriptedEpochLog({
      epochs: [
        [4, '0'],
        [5, '300'],
      ],
      logEnd: '500',
    })
    // The first check is answered by the leader at epoch 5 once released; a later one is still
    // unanswered when the test stops the run.
    let release
    const released = new Promise((resolve) => (release = resolve))
    let checks = 0
    h.setValidationResponse((partitions, request) => {
      checks += 1
      return checks === 1
        ? released.then(() => atEpoch5.answer(partitions, request))
        : new Promise(() => {})
    })
    try {
      const round = h.group.fetch(NODE_ID)
      await until(() => h.validationRequests().length === 1)

      // The leader moves to node 1 at epoch 6; then an older image that no longer lists node 1
      // names node 0 at epoch 5 again, and the cache accepts it.
      h.metadataCache.applyLeaderHint(TOPIC, 0, { leaderId: 1, leaderEpoch: 6 })
      h.metadataCache.update({
        brokers: [{ nodeId: NODE_ID, host: 'kafka0', port: 9092 }],
        topicMetadata: [
          {
            topic: TOPIC,
            topicId: TOPIC_ID,
            partitionMetadata: [0, 1].map((partitionId) => ({
              partitionId,
              leader: NODE_ID,
              leaderEpoch: 5,
              offlineReplicas: [],
            })),
          },
        ],
      })
      expect([h.metadataCache.leader(TOPIC, 0), h.metadataCache.leaderEpoch(TOPIC, 0)]).toEqual([
        NODE_ID,
        5,
      ])
      release()
      await until(() => h.validationRequests().length === 2 || h.fetchFrames().length > 0)

      expect(h.fetchFrames()).toEqual([])
      expect(h.validationRequests().map(({ topics }) => topics[0].partitions)).toEqual([
        [
          { partition: 0, currentLeaderEpoch: 5, leaderEpoch: 4 },
          { partition: 1, currentLeaderEpoch: 5, leaderEpoch: 4 },
        ],
        [{ partition: 0, currentLeaderEpoch: 5, leaderEpoch: 4 }],
      ])

      h.group.onStopRequested()
      await expect(round).resolves.toEqual([])
    } finally {
      await h.teardown()
    }
  })
})
