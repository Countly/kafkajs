const {
  NODE_ID,
  TOPIC,
  prepareRecordBatches,
  fetchResponse,
  createFetchGroup,
} = require('./fixtures/fetchHarness')

beforeAll(prepareRecordBatches)

describe('Consumer > fetch partition error contract', () => {
  describe('one case per partition-table row', () => {
    test('OFFSET_OUT_OF_RANGE (1) resets only that partition, and the others are delivered', async () => {
      const { group, offsetManager, joinAndSync, setResponse } = await createFetchGroup({
        partitions: [0, 1],
      })
      // The real reset commits through a coordinator this harness has no broker for; what the
      // contract asserts is that the per-partition recovery is the one that runs.
      const setDefaultOffset = jest
        .spyOn(offsetManager, 'setDefaultOffset')
        .mockResolvedValue(undefined)

      setResponse(
        fetchResponse([
          { partition: 0, errorCode: 1 },
          { partition: 1, errorCode: 0 },
        ])
      )
      const batches = await group.fetch(NODE_ID)

      expect(setDefaultOffset).toHaveBeenCalledWith({ topic: TOPIC, partition: 0 })
      expect(batches.map(({ partition }) => partition)).toEqual([1])
      expect(joinAndSync).not.toHaveBeenCalled()
    })

    test.each([
      [3, 'UNKNOWN_TOPIC_OR_PARTITION'],
      [5, 'LEADER_NOT_AVAILABLE'],
      [6, 'NOT_LEADER_FOR_PARTITION'],
      [9, 'REPLICA_NOT_AVAILABLE'],
      [56, 'KAFKA_STORAGE_ERROR'],
      [74, 'FENCED_LEADER_EPOCH'],
      [100, 'UNKNOWN_TOPIC_ID'],
      [103, 'INCONSISTENT_TOPIC_ID'],
      [106, 'FETCH_SESSION_TOPIC_ID_ERROR'],
    ])(
      '%i (%s) refreshes metadata once, delivers the other partitions and never rejoins',
      async (errorCode) => {
        const { group, cluster, joinAndSync, setResponse } = await createFetchGroup({
          partitions: [0, 1],
        })

        setResponse(
          fetchResponse([
            { partition: 0, errorCode },
            { partition: 1, errorCode: 0 },
          ])
        )
        const batches = await group.fetch(NODE_ID)

        expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
        expect(batches.map(({ partition }) => partition)).toEqual([1])
        expect(joinAndSync).not.toHaveBeenCalled()
      }
    )

    test('UNKNOWN_LEADER_EPOCH (75) forgets the cached epoch, warns, does not refresh, and converges', async () => {
      const { group, cluster, logger, setResponse, lastFetchRequest } = await createFetchGroup({
        partitions: [0],
      })
      const warn = jest.spyOn(logger, 'warn')

      setResponse(fetchResponse([{ partition: 0, errorCode: 75 }]))
      await group.fetch(NODE_ID)

      expect(cluster.forgetLeaderEpoch).toHaveBeenCalledWith(TOPIC, 0)
      expect(cluster.refreshMetadata).not.toHaveBeenCalled()
      // Rewriting cached cluster state leaves a trace even when it clears on the next round.
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining('leader epoch'),
        expect.objectContaining({ topic: TOPIC, partition: 0 })
      )

      // Second round: the epoch is unknown, so the request carries -1 and the broker accepts it.
      cluster.leaderEpoch = () => -1
      setResponse(fetchResponse([{ partition: 0, errorCode: 0 }]))
      const batches = await group.fetch(NODE_ID)
      expect(batches).toHaveLength(1)
      expect(lastFetchRequest()[0].currentLeaderEpoch).toEqual(-1)
    })

    test('OFFSET_NOT_AVAILABLE (78) skips the partition, keeps its preferred replica, does not refresh', async () => {
      const { group, cluster, setResponse } = await createFetchGroup({ partitions: [0] })
      group.preferredReadReplicasPerTopicPartition[TOPIC] = {
        0: { nodeId: 2, expireAt: Date.now() + 60000 },
      }
      // A steady-state round: the partition's position was already checked against the leader
      // epoch the cluster reports, so entering position validation is not what drops the
      // preference here - only the skip could.
      group.positionValidator.markValidated(TOPIC, 0, cluster.leaderEpoch())

      setResponse(fetchResponse([{ partition: 0, errorCode: 78 }]))
      // The round is addressed to the preferred replica: a partition with a live preference is
      // fetched from that node, not from its leader.
      const batches = await group.fetch(2)

      expect(batches).toEqual([])
      expect(cluster.refreshMetadata).not.toHaveBeenCalled()
      expect(group.preferredReadReplicasPerTopicPartition[TOPIC][0]).toEqual(
        expect.objectContaining({ nodeId: 2 })
      )
      // The skip really ran, rather than the round never reaching the response walk.
      expect([...group.consecutiveFetchErrors.keys()]).toEqual([`${TOPIC}|0`])
    })

    test('OFFSET_MOVED_TO_TIERED_STORAGE (109) is thrown, as any unhandled partition error is', async () => {
      const { group, setResponse } = await createFetchGroup({ partitions: [0] })

      setResponse(fetchResponse([{ partition: 0, errorCode: 109 }]))

      // `ConsumerGroup#fetch` catches, `recoverFromFetch` matches no recovery branch, and its tail
      // re-throws - so an unhandled partition code rejects the round, exactly as before v12.
      await expect(group.fetch(NODE_ID)).rejects.toMatchObject({
        type: 'OFFSET_MOVED_TO_TIERED_STORAGE',
      })
    })

    test('an unmapped partition code keeps the previous behaviour and is thrown', async () => {
      const { group, setResponse } = await createFetchGroup({ partitions: [0] })

      setResponse(fetchResponse([{ partition: 0, errorCode: 29 }])) // TOPIC_AUTHORIZATION_FAILED

      await expect(group.fetch(NODE_ID)).rejects.toMatchObject({
        type: 'TOPIC_AUTHORIZATION_FAILED',
      })
    })
  })

  describe('one case per top-level-table row', () => {
    test.each([[70], [71], [106]])(
      'the fetch-session code %i warns, refreshes once and yields an empty round without crashing',
      async (errorCode) => {
        const { group, cluster, logger, setResponse } = await createFetchGroup({ partitions: [0] })
        const warn = jest.spyOn(logger, 'warn')

        setResponse(fetchResponse([{ partition: 0, errorCode: 0 }], { topLevel: errorCode }))
        const batches = await group.fetch(NODE_ID)

        expect(batches).toEqual([])
        expect(warn).toHaveBeenCalledWith(
          expect.stringContaining('top-level'),
          expect.objectContaining({ errorCode })
        )
        expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
      }
    )

    test('an unmapped top-level code is thrown by the consumer, never by parse', async () => {
      const { group, setResponse } = await createFetchGroup({ partitions: [0] })
      const { parse } = require('../../protocol/requests/fetch/v12/parse')

      // parse resolves: it is the consumer that classifies and throws.
      await expect(parse({ errorCode: 41, responses: [] })).resolves.toBeTruthy()

      setResponse(fetchResponse([{ partition: 0, errorCode: 0 }], { topLevel: 41 }))

      await expect(group.fetch(NODE_ID)).rejects.toMatchObject({ code: 41 })
    })
  })

  test('a partition error does not discard the other partitions batches', async () => {
    const { group, setResponse } = await createFetchGroup({ partitions: [0, 1, 2] })

    setResponse(
      fetchResponse([
        { partition: 0, errorCode: 6, records: false },
        { partition: 1, errorCode: 0, records: true },
        { partition: 2, errorCode: 0, records: true },
      ])
    )
    const batches = await group.fetch(NODE_ID)

    expect(batches.map(({ partition }) => partition)).toEqual([1, 2])
  })

  test('exactly one metadata refresh runs however many partitions ask for one', async () => {
    const { group, cluster, setResponse } = await createFetchGroup({ partitions: [0, 1, 2] })

    setResponse(
      fetchResponse([
        { partition: 0, errorCode: 6 },
        { partition: 1, errorCode: 3 },
        { partition: 2, errorCode: 100 },
      ])
    )
    await group.fetch(NODE_ID)

    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })

  test('an errored partition is not resolved: an already resolved offset is untouched', async () => {
    const { group, offsetManager, setResponse, lastFetchRequest } = await createFetchGroup({
      partitions: [0],
    })

    setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: true }]))
    await group.fetch(NODE_ID)
    // What the runner does once a batch has been handled: the position moves past offset 0.
    offsetManager.resolveOffset({ topic: TOPIC, partition: 0, offset: '0' })

    setResponse(fetchResponse([{ partition: 0, errorCode: 6 }]))
    await group.fetch(NODE_ID)
    setResponse(fetchResponse([{ partition: 0, errorCode: 0 }]))
    await group.fetch(NODE_ID)

    // Neither reset to the start nor advanced past a batch that never arrived.
    expect(lastFetchRequest()[0].fetchOffset).toEqual('1')
  })

  test('a partition failing every round eventually warns, and the counter resets on delivery', async () => {
    const { group, logger, setResponse } = await createFetchGroup({ partitions: [0] })
    const warn = jest.spyOn(logger, 'warn')
    const failing = () => fetchResponse([{ partition: 0, errorCode: 78 }])

    for (let round = 1; round <= 4; round++) {
      setResponse(failing())
      await group.fetch(NODE_ID)
    }
    expect(warn).not.toHaveBeenCalledWith(expect.stringContaining('consecutive'), expect.anything())

    setResponse(failing())
    await group.fetch(NODE_ID)
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining('consecutive'),
      expect.objectContaining({ topic: TOPIC, partition: 0, errorCode: 78, consecutiveRounds: 5 })
    )

    warn.mockClear()
    for (let round = 6; round <= 29; round++) {
      setResponse(failing())
      await group.fetch(NODE_ID)
    }
    expect(warn).not.toHaveBeenCalledWith(expect.stringContaining('consecutive'), expect.anything())

    setResponse(failing())
    await group.fetch(NODE_ID)
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining('consecutive'),
      expect.objectContaining({ consecutiveRounds: 30 })
    )

    warn.mockClear()
    setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: true }]))
    await group.fetch(NODE_ID)
    setResponse(failing())
    await group.fetch(NODE_ID)
    expect(warn).not.toHaveBeenCalledWith(expect.stringContaining('consecutive'), expect.anything())

    // The delivery restarted the count rather than merely leaving the warning predicate unmet:
    // without the reset the counter would stand at 31 here, and the four rounds below would pass
    // it in silence instead of crossing the five-round threshold again.
    expect(group.consecutiveFetchErrors.get(`${TOPIC}|0`)).toEqual(1)
    for (let round = 2; round <= 5; round++) {
      setResponse(failing())
      await group.fetch(NODE_ID)
    }
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining('consecutive'),
      expect.objectContaining({ consecutiveRounds: 5 })
    )
  })

  test('a failing partition leaves exactly one counter entry, keyed by topic and partition', async () => {
    const { group, setResponse } = await createFetchGroup({ partitions: [0, 1] })
    setResponse(
      fetchResponse([
        { partition: 0, errorCode: 78 },
        { partition: 1, errorCode: 0, records: true },
      ])
    )
    await group.fetch(NODE_ID)

    // That a sync clears this map is asserted where a real sync can be driven; clearing it here
    // and asserting it is empty would be a test that cannot fail.
    expect([...group.consecutiveFetchErrors.keys()]).toEqual([`${TOPIC}|0`])
  })
  describe('debug logging', () => {
    it('records what was asked for and what came back', async () => {
      const { group, logger, setResponse } = await createFetchGroup({ partitions: [0] })
      jest.spyOn(logger, 'debugEnabled').mockReturnValue(true)
      const debug = jest.spyOn(logger, 'debug')

      setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: true }]))
      await group.fetch(NODE_ID)

      expect(debug).toHaveBeenCalledWith(
        'Fetching',
        expect.objectContaining({
          nodeId: NODE_ID,
          topics: [
            expect.objectContaining({
              topic: TOPIC,
              partitions: [expect.objectContaining({ partition: 0 })],
            }),
          ],
        })
      )
      expect(debug).toHaveBeenCalledWith(
        'Fetch response partition',
        expect.objectContaining({ topic: TOPIC, partition: 0, errorCode: 0 })
      )
    })

    it('names the reason a fetched partition was dropped', async () => {
      const { group, logger, setResponse } = await createFetchGroup({ partitions: [0] })
      const debug = jest.spyOn(logger, 'debug')
      // A seek queued between the request going out and its response arriving: the response is
      // for a position the consumer no longer wants, which is exactly what the drop reason names.
      jest.spyOn(group.seekOffset, 'has').mockReturnValue(true)

      setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: true }]))
      const batches = await group.fetch(NODE_ID)

      expect(batches).toEqual([])
      expect(debug).toHaveBeenCalledWith('Dropping fetched partition', {
        topic: TOPIC,
        partition: 0,
        reason: 'seeked',
      })
    })

    it('does not build the fetching payload at the default level', async () => {
      const { group, logger, setResponse } = await createFetchGroup({ partitions: [0] })
      const debug = jest.spyOn(logger, 'debug')
      jest.spyOn(logger, 'debugEnabled').mockReturnValue(false)

      setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: true }]))
      await group.fetch(NODE_ID)

      expect(debug).not.toHaveBeenCalledWith('Fetching', expect.anything())
      expect(debug).not.toHaveBeenCalledWith('Resolved offsets', expect.anything())
      expect(debug).not.toHaveBeenCalledWith('Fetch response partition', expect.anything())
    })
  })
})
