const {
  TOPIC,
  NODE_ID,
  createFetchGroup,
  prepareRecordBatches,
  fetchResponse,
} = require('./fixtures/fetchHarness')

let harness
beforeAll(() => prepareRecordBatches())
afterEach(async () => {
  if (harness) await harness.broker.disconnect()
  harness = null
  jest.useRealTimers()
})

/** @returns {Promise<void>} yields one event-loop turn without arming a timer */
const turn = () => new Promise(setImmediate)

/**
 * @param {Promise<*>} promise a round that may never settle
 * @returns {Promise<*>} its value, or 'pending' when it has not settled shortly
 */
const settled = (promise) =>
  Promise.race([promise, new Promise((resolve) => setTimeout(resolve, 250, 'pending'))])

/**
 * Holds every metadata refresh until released.
 * @param {object} cluster the harness cluster
 * @returns {{ release: (index?: number) => void, options: object[] }} the release of one held call,
 *   or of all of them, and the options each held call got
 */
const holdRefreshes = (cluster) => {
  const releases = []
  const options = []
  cluster.refreshMetadata.mockImplementation((given) => {
    options.push(given)
    return new Promise((resolve) => releases.push(resolve))
  })
  return {
    release: (index) =>
      (index === undefined ? releases.splice(0) : releases.splice(index, 1)).forEach((resolve) =>
        resolve()
      ),
    options,
  }
}

describe('routing with an unavailable leader', () => {
  it('fetches the healthy sibling and requests no rejoin', async () => {
    harness = await createFetchGroup({
      partitions: [0, 1],
      partitionLeaders: { 0: -1, 1: NODE_ID },
    })
    harness.setResponse(fetchResponse([{ partition: 1, errorCode: 0, records: true }]))

    const batches = await harness.group.fetch(NODE_ID)

    expect(batches.map(({ partition }) => partition)).toEqual([1])
    expect(harness.fetchRequestPartitions()).toEqual([1])
    expect(harness.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    expect(harness.requestRejoin).not.toHaveBeenCalled()
  })

  it('still delivers the healthy sibling when the routing refresh fails', async () => {
    harness = await createFetchGroup({
      partitions: [0, 1],
      partitionLeaders: { 0: -1, 1: NODE_ID },
    })
    harness.cluster.refreshMetadata.mockRejectedValueOnce(new Error('metadata disconnected'))
    harness.setResponse(fetchResponse([{ partition: 1, errorCode: 0, records: true }]))

    const batches = await harness.group.fetch(NODE_ID)

    expect(batches.map(({ partition }) => partition)).toEqual([1])
    expect(harness.requestRejoin).not.toHaveBeenCalled()
  })

  it('refreshes once for the response error and the missing leader together', async () => {
    harness = await createFetchGroup({
      partitions: [0, 1, 2],
      partitionLeaders: { 0: -1, 1: NODE_ID, 2: NODE_ID },
    })
    harness.setResponse(
      fetchResponse([
        { partition: 1, errorCode: 6 },
        { partition: 2, errorCode: 0, records: true },
      ])
    )

    const batches = await harness.group.fetch(NODE_ID)

    expect(batches.map(({ partition }) => partition)).toEqual([2])
    expect(harness.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })

  it('shares one refresh across the nodes and idles them without a request or timer leak', async () => {
    harness = await createFetchGroup({
      partitions: [0],
      partitionLeaders: { 0: -1 },
      retry: { initialRetryTime: 100, retries: 0 },
    })
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] })

    const rounds = [0, 1, 2].map((node) => harness.group.fetch(node))
    await jest.advanceTimersByTimeAsync(100)

    expect(await Promise.all(rounds)).toEqual([[], [], []])
    expect(harness.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    expect(harness.requests().filter(({ apiKey }) => apiKey === 1)).toEqual([])
    expect(harness.requestRejoin).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })

  it('spaces refreshes by the initial retry time and keeps no timer of its own', async () => {
    harness = await createFetchGroup({
      partitions: [0],
      partitionLeaders: { 0: -1 },
      retry: { initialRetryTime: 300, retries: 0 },
    })
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] })

    const counts = []
    for (let round = 0; round < 4; round++) {
      const pending = harness.group.fetch(NODE_ID)
      await jest.advanceTimersByTimeAsync(100)
      await pending
      counts.push(harness.cluster.refreshMetadata.mock.calls.length)
    }

    expect(counts).toEqual([1, 1, 1, 2])
    expect(jest.getTimerCount()).toBe(0)
  })

  it('coalesces a held refresh across nodes even with no spacing', async () => {
    harness = await createFetchGroup({
      partitions: [0],
      partitionLeaders: { 0: -1 },
      retry: { initialRetryTime: 0, retries: 0 },
    })
    const held = holdRefreshes(harness.cluster)

    const first = harness.group.fetch(0)
    const second = harness.group.fetch(1)
    await turn()
    await turn()
    expect(harness.cluster.refreshMetadata).toHaveBeenCalledTimes(1)

    held.release()
    await Promise.all([first, second])
  })

  it('includes a partition in the round after its leader returns', async () => {
    harness = await createFetchGroup({
      partitions: [0, 1],
      partitionLeaders: { 0: -1, 1: NODE_ID },
    })
    harness.setResponse(fetchResponse([{ partition: 1, errorCode: 0, records: true }]))
    await harness.group.fetch(NODE_ID)

    harness.installRouting({ 0: NODE_ID })
    harness.setResponse(
      fetchResponse([0, 1].map((partition) => ({ partition, errorCode: 0, records: true })))
    )
    const batches = await harness.group.fetch(NODE_ID)

    expect(batches.map(({ partition }) => partition).sort()).toEqual([0, 1])
    expect(harness.fetchRequestPartitions().sort()).toEqual([0, 1])
    expect(harness.requestRejoin).not.toHaveBeenCalled()
  })

  it('still rejoins when a subscribed topic gained partitions beside the missing leader', async () => {
    harness = await createFetchGroup({
      partitions: [0, 1],
      partitionLeaders: { 0: -1, 1: NODE_ID },
    })
    harness.group.partitionsPerSubscribedTopic = new Map([[TOPIC, [0]]])

    await harness.group.fetch(NODE_ID)

    expect(harness.requestRejoin).toHaveBeenCalledTimes(1)
    expect(harness.requestRejoin).toHaveBeenCalledWith(
      expect.objectContaining({
        reason: 'stale-assignment',
        error: expect.objectContaining({ name: 'KafkaJSStaleTopicMetadataAssignment' }),
      })
    )
  })

  it('keeps an invalid metadata row an error rather than a missing leader', async () => {
    harness = await createFetchGroup({ partitions: [0] })
    harness.cluster.findTopicPartitionMetadata = () => [{ partitionId: 0, leader: null }]

    expect(() => harness.group.findReadReplicaForPartitions(TOPIC, [0])).toThrow(
      /Invalid partition metadata/
    )
  })

  it.each([
    [[1], 0],
    [[], 1],
    [[0], 1],
    [[0, 1], 0],
  ])('selects the eligible node with offline replicas %j', async (offlineReplicas, expected) => {
    harness = await createFetchGroup({ preferredReadReplica: 1, knownNodes: [0, 1] })
    harness.cluster.findTopicPartitionMetadata = () => [
      { partitionId: 0, leader: 0, offlineReplicas },
    ]

    expect(harness.group.findReadReplicaForPartitions(TOPIC, [0])).toEqual({ [expected]: [0] })
  })

  it('expires a preference and omits a partition that has no metadata', async () => {
    harness = await createFetchGroup({ preferredReadReplica: 1, knownNodes: [0, 1] })
    harness.group.preferredReadReplicasPerTopicPartition[TOPIC][0].expireAt = Date.now() - 1

    expect(harness.group.findReadReplicaForPartitions(TOPIC, [0])).toEqual({ 0: [0] })
    expect(harness.group.preferredReadReplicasPerTopicPartition[TOPIC][0]).toBeUndefined()

    harness.cluster.findTopicPartitionMetadata = () => []
    expect(harness.group.findReadReplicaForPartitions(TOPIC, [0])).toEqual({})
  })
})

describe('a routing refresh and its run', () => {
  /**
   * Starts a fetch round of an all-leaderless group and waits until its refresh was dispatched.
   * @param {number} [node] the node that asks
   * @returns {Promise<{ round: Promise<object[]>, held: object, run: object }>} the round, the held
   *   refresh and the run that owns it
   */
  const heldRound = async (node = NODE_ID) => {
    const held = holdRefreshes(harness.cluster)
    const run = harness.group.idleWait
    const round = harness.group.fetch(node)
    for (let attempt = 0; attempt < 10 && held.options.length === 0; attempt++) await turn()
    return { round, held, run }
  }

  it('binds the refresh to the run and settles the round when the run stops', async () => {
    harness = await createFetchGroup({ partitions: [0], partitionLeaders: { 0: -1 } })
    const { round, held, run } = await heldRound()

    expect(held.options[0]).toMatchObject({ fence: run.fence })
    expect(run.fence.closed).toBe(false)

    harness.group.onStopRequested()

    expect(await settled(round)).toEqual([])
    expect(run.fence.closed).toBe(true)
    held.release()
    await turn()
    expect(harness.requestRejoin).not.toHaveBeenCalled()
  })

  it('starts no refresh for a run that already stopped', async () => {
    harness = await createFetchGroup({ partitions: [0], partitionLeaders: { 0: -1 } })
    const run = harness.group.idleWait
    harness.group.onStopRequested()

    await harness.group.refreshLeaderlessMetadata(run)

    expect(harness.cluster.refreshMetadata).not.toHaveBeenCalled()
  })

  it('lets a replacement run refresh at once and keeps it safe from the old refresh settling', async () => {
    harness = await createFetchGroup({
      partitions: [0],
      partitionLeaders: { 0: -1 },
      retry: { initialRetryTime: 60000, retries: 0 },
    })
    const old = await heldRound()
    harness.group.startIdleWait()
    const replacement = harness.group.idleWait

    const next = harness.group.fetch(NODE_ID)
    for (let attempt = 0; attempt < 10 && old.held.options.length < 2; attempt++) await turn()
    expect(old.held.options).toHaveLength(2)
    expect(old.held.options[1]).toMatchObject({ fence: replacement.fence })
    const pending = harness.group.leaderlessRefresh

    expect(await settled(old.round)).toEqual([])
    expect(harness.group.leaderlessRefresh).toBe(pending)

    old.held.release(0)
    await turn()
    expect(harness.group.leaderlessRefresh).toBe(pending)

    old.held.release()
    expect(await settled(next)).toEqual([])
    expect(harness.group.leaderlessRefresh).toBeNull()
    expect(harness.requestRejoin).not.toHaveBeenCalled()
  })

  it('does not let a stale run share the replacement run refresh', async () => {
    harness = await createFetchGroup({ partitions: [0], partitionLeaders: { 0: -1 } })
    const oldRun = harness.group.idleWait
    harness.group.startIdleWait()
    const held = holdRefreshes(harness.cluster)

    const current = harness.group.refreshLeaderlessMetadata(harness.group.idleWait)
    await turn()
    await harness.group.refreshLeaderlessMetadata(oldRun)

    expect(held.options).toHaveLength(1)
    held.release()
    await current
  })
})
