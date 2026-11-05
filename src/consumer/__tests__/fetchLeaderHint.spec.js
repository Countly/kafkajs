const createFetchManager = require('../fetchManager')
const { newLogger } = require('testHelpers')
const {
  NODE_ID,
  TOPIC,
  TOPIC_ID,
  prepareRecordBatches,
  fetchResponse,
  createFetchGroup,
} = require('./fixtures/fetchHarness')

/** @type {Buffer} */
let CORRUPTED_RECORD_BATCH
/** @type {Buffer} */
let ABORTED_AND_COMMITTED_BATCHES

beforeAll(async () => {
  ;({ CORRUPTED_RECORD_BATCH, ABORTED_AND_COMMITTED_BATCHES } = await prepareRecordBatches())
})

describe('Consumer > KIP-951 leader hints on fetch', () => {
  test('a CurrentLeader hint re-routes the next round without a rejoin', async () => {
    const { group, cluster, requestRejoin, setResponse } = await createFetchGroup({
      partitions: [0],
    })

    setResponse(
      fetchResponse(
        [{ partition: 0, errorCode: 6, currentLeader: { leaderId: 2, leaderEpoch: 9 } }],
        {
          nodeEndpoints: [{ nodeId: 2, host: 'kafka2', port: 9095, rack: null }],
        }
      )
    )
    await group.fetch(NODE_ID)

    expect(cluster.applyLeaderHint).toHaveBeenCalledWith(
      {
        topic: TOPIC,
        partition: 0,
        leaderId: 2,
        leaderEpoch: 9,
        nodeEndpoints: [{ nodeId: 2, host: 'kafka2', port: 9095, rack: null }],
      },
      { fence: group.idleWait.fence }
    )
    // A usable hint is the refresh: nothing else is needed this round.
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(requestRejoin).not.toHaveBeenCalled()
  })

  test('a hint with no endpoints naming a KNOWN node id is applied', async () => {
    const { group, cluster, setResponse } = await createFetchGroup({
      partitions: [0],
      knownNodes: [0, 2],
    })

    setResponse(
      fetchResponse([
        { partition: 0, errorCode: 74, currentLeader: { leaderId: 2, leaderEpoch: 9 } },
      ])
    )
    await group.fetch(NODE_ID)

    expect(cluster.hasNode).toHaveBeenCalledWith(2)
    expect(cluster.applyLeaderHint).toHaveBeenCalledWith(
      expect.objectContaining({ leaderId: 2, leaderEpoch: 9, nodeEndpoints: undefined }),
      { fence: group.idleWait.fence }
    )
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })

  test('a hint with no endpoints naming an UNKNOWN node id refreshes instead of stranding the partition', async () => {
    const { group, cluster, setResponse } = await createFetchGroup({
      partitions: [0],
      knownNodes: [0],
    })

    setResponse(
      fetchResponse([
        { partition: 0, errorCode: 6, currentLeader: { leaderId: 7, leaderEpoch: 9 } },
      ])
    )
    await group.fetch(NODE_ID)

    expect(cluster.applyLeaderHint).not.toHaveBeenCalled()
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })

  test('the node-id membership test survives the string/number boundary', async () => {
    // BrokerPool keys its broker map with Object.keys, i.e. strings, while a decoded LeaderId is a
    // number. Both cases run side by side so a coercion bug cannot make them both pass.
    const { group, cluster, setResponse } = await createFetchGroup({
      partitions: [0],
      knownNodes: [0, 2],
    })

    setResponse(
      fetchResponse([
        { partition: 0, errorCode: 6, currentLeader: { leaderId: 2, leaderEpoch: 9 } },
      ])
    )
    await group.fetch(NODE_ID)
    expect(cluster.applyLeaderHint).toHaveBeenCalled()

    cluster.applyLeaderHint.mockClear()
    setResponse(
      fetchResponse([
        { partition: 0, errorCode: 6, currentLeader: { leaderId: 99, leaderEpoch: 10 } },
      ])
    )
    await group.fetch(NODE_ID)
    expect(cluster.applyLeaderHint).not.toHaveBeenCalled()
  })

  test('a leaderId of -1 is no hint at all', async () => {
    const { group, cluster, setResponse } = await createFetchGroup({ partitions: [0] })

    setResponse(
      fetchResponse([
        { partition: 0, errorCode: 6, currentLeader: { leaderId: -1, leaderEpoch: -1 } },
      ])
    )
    await group.fetch(NODE_ID)

    expect(cluster.applyLeaderHint).not.toHaveBeenCalled()
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })

  test('endpoints that do not name the hinted leader fall back to the membership test', async () => {
    // A response can move several partitions and omit the listener for one of their leaders. The
    // endpoints are still the freshest addresses for the nodes they do name, so they are
    // forwarded - but they vouch for node 5, not for the leader, which is why the hint is only
    // applied here because the client already knows node 2.
    const { group, cluster, setResponse } = await createFetchGroup({
      partitions: [0],
      knownNodes: [0, 2],
    })
    const otherNodeEndpoints = [{ nodeId: 5, host: 'kafka5', port: 9098, rack: null }]

    setResponse(
      fetchResponse(
        [{ partition: 0, errorCode: 6, currentLeader: { leaderId: 2, leaderEpoch: 9 } }],
        { nodeEndpoints: otherNodeEndpoints }
      )
    )
    await group.fetch(NODE_ID)

    expect(cluster.hasNode).toHaveBeenCalledWith(2)
    expect(cluster.applyLeaderHint).toHaveBeenCalledWith(
      expect.objectContaining({ leaderId: 2, nodeEndpoints: otherNodeEndpoints }),
      { fence: group.idleWait.fence }
    )
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })

  test('endpoints that do not name an UNKNOWN hinted leader refresh instead', async () => {
    const { group, cluster, setResponse } = await createFetchGroup({
      partitions: [0],
      knownNodes: [0],
    })

    setResponse(
      fetchResponse(
        [{ partition: 0, errorCode: 6, currentLeader: { leaderId: 7, leaderEpoch: 9 } }],
        { nodeEndpoints: [{ nodeId: 5, host: 'kafka5', port: 9098, rack: null }] }
      )
    )
    await group.fetch(NODE_ID)

    expect(cluster.applyLeaderHint).not.toHaveBeenCalled()
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })

  test('a hint the cache rejects refreshes, and the round still delivers the other partitions', async () => {
    // What a non-advancing leader epoch looks like from here: the hint is well formed and reaches
    // the cluster, the cluster declines it, and the round falls back to one refresh.
    const { group, cluster, setResponse } = await createFetchGroup({
      partitions: [0, 1],
      applyLeaderHintResolves: false,
    })

    setResponse(
      fetchResponse(
        [
          { partition: 0, errorCode: 6, currentLeader: { leaderId: 2, leaderEpoch: 4 } },
          { partition: 1, errorCode: 0, records: true },
        ],
        { nodeEndpoints: [{ nodeId: 2, host: 'kafka2', port: 9095, rack: null }] }
      )
    )
    const batches = await group.fetch(NODE_ID)

    expect(cluster.applyLeaderHint).toHaveBeenCalledTimes(1)
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    expect(batches.map(({ partition }) => partition)).toEqual([1])
  })

  test('a v12 hint naming a KNOWN node id is applied, with no endpoints on the wire at all', async () => {
    // Below v16 the response carries no NodeEndpoints field, so the decoder omits the key rather
    // than answering with an empty array. This drives that frame for real.
    const { group, cluster, setResponse, lastFetchApiVersion } = await createFetchGroup({
      partitions: [0],
      knownNodes: [0, 2],
      fetchMaxVersion: 12,
    })

    setResponse(
      fetchResponse(
        [{ partition: 0, errorCode: 6, currentLeader: { leaderId: 2, leaderEpoch: 9 } }],
        { apiVersion: 12 }
      )
    )
    await group.fetch(NODE_ID)

    expect(lastFetchApiVersion()).toEqual(12)
    expect(cluster.hasNode).toHaveBeenCalledWith(2)
    expect(cluster.applyLeaderHint).toHaveBeenCalledWith(
      expect.objectContaining({ leaderId: 2, leaderEpoch: 9, nodeEndpoints: undefined }),
      { fence: group.idleWait.fence }
    )
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })

  test('a v12 hint naming an UNKNOWN node id refreshes instead', async () => {
    const { group, cluster, setResponse, lastFetchApiVersion } = await createFetchGroup({
      partitions: [0],
      knownNodes: [0],
      fetchMaxVersion: 12,
    })

    setResponse(
      fetchResponse(
        [{ partition: 0, errorCode: 6, currentLeader: { leaderId: 7, leaderEpoch: 9 } }],
        { apiVersion: 12 }
      )
    )
    await group.fetch(NODE_ID)

    expect(lastFetchApiVersion()).toEqual(12)
    expect(cluster.applyLeaderHint).not.toHaveBeenCalled()
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })

  test('the next round routes to the hinted node', async () => {
    const { group, findBrokerCalls, setResponse } = await createFetchGroup({
      partitions: [0],
      applyLeaderHintMovesLeaderTo: 2,
    })

    setResponse(
      fetchResponse(
        [{ partition: 0, errorCode: 6, currentLeader: { leaderId: 2, leaderEpoch: 9 } }],
        {
          nodeEndpoints: [{ nodeId: 2, host: 'kafka2', port: 9095, rack: null }],
        }
      )
    )
    await group.fetch(NODE_ID)

    setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: true }]))
    await group.fetch(2)

    expect(findBrokerCalls.map(({ nodeId }) => nodeId)).toEqual([NODE_ID, 2])
  })
})

describe('Consumer > a hint that adds an unknown node rebalances the fetchers, not the group', () => {
  test('surviving fetchers stay in place and no batch is lost when a node is added', async () => {
    const nodeIds = [0]
    const delivered = []
    const fetchedNodes = new Set()
    let round = 0

    const fetchManager = createFetchManager({
      logger: newLogger(),
      getNodeIds: () => [...nodeIds],
      fetch: async (nodeId) => {
        fetchedNodes.add(nodeId)
        round += 1
        if (round === 1) {
          // The hint added a node the client had never heard of.
          nodeIds.push(2)
          return [{ topic: TOPIC, partition: 0, value: 'a' }]
        }
        if (round > 4) {
          await new Promise((resolve) => setTimeout(resolve, 10))
          return []
        }
        return [{ topic: TOPIC, partition: nodeId, value: `r${round}` }]
      },
      handler: async (batch) => delivered.push(batch.value),
    })

    const started = fetchManager.start()
    const [survivor] = fetchManager.getFetchers()
    try {
      expect(survivor).toBeDefined()
      await new Promise((resolve) => setTimeout(resolve, 200))

      expect([...fetchedNodes].sort()).toEqual([0, 2])
      const fetchers = fetchManager.getFetchers()
      expect(fetchers).toHaveLength(2)
      expect(fetchers[0]).toBe(survivor)
      expect(fetchers[1]).not.toBe(survivor)
      expect(delivered).toContain('a')
      expect(delivered).toContain('r2')
    } finally {
      await fetchManager.stop()
      await started
    }
    expect(fetchManager.getFetchers()).toEqual([])
  })
})

describe('Consumer > v11 behaviour is unchanged at v18', () => {
  test('maxBytesPerPartition reaches PartitionMaxBytes on the wire', async () => {
    const { group, setResponse, lastFetchRequest } = await createFetchGroup({ partitions: [0] })
    group.maxBytesPerPartition = 4096

    setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: true }]))
    await group.fetch(NODE_ID)

    expect(lastFetchRequest()[0].maxBytes).toEqual(4096)
  })

  test('rackId and isolationLevel are sent', async () => {
    const { group, setResponse, lastFetchHeaderFields } = await createFetchGroup({
      partitions: [0],
    })
    group.rackId = 'eu-west-1a'
    group.isolationLevel = 1

    setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: true }]))
    await group.fetch(NODE_ID)

    expect(lastFetchHeaderFields()).toMatchObject({ rackId: 'eu-west-1a', isolationLevel: 1 })
  })

  test('checkCrcs reports a corrupted batch as a checksum corruption, not as a rejected fetch', async () => {
    const { broker, setResponse } = await createFetchGroup({ partitions: [0], checkCrcs: true })

    setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: CORRUPTED_RECORD_BATCH }]))

    const { responses } = await broker.fetch({
      topics: [
        { topic: TOPIC, partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1048576 }] },
      ],
      topicIds: new Map([[TOPIC, TOPIC_ID]]),
      checkCrcs: true,
    })

    expect(responses[0].partitions[0].corruption).toMatchObject({ reason: 'checksum' })
  })

  test('checkCrcs: false delivers the same corrupted batch instead of crashing', async () => {
    const { group, setResponse } = await createFetchGroup({ partitions: [0], checkCrcs: false })

    setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: CORRUPTED_RECORD_BATCH }]))

    await expect(group.fetch(NODE_ID)).resolves.toHaveLength(1)
  })

  test('preferredReadReplica is honoured, and expires at metadataMaxAge', async () => {
    const { group, setResponse } = await createFetchGroup({ partitions: [0] })

    setResponse(
      fetchResponse([{ partition: 0, errorCode: 0, records: true, preferredReadReplica: 2 }])
    )
    await group.fetch(NODE_ID)
    expect(group.preferredReadReplicasPerTopicPartition[TOPIC][0]).toMatchObject({ nodeId: 2 })

    // The next round routes to the replica...
    expect(group.findReadReplicaForPartitions(TOPIC, [0])).toEqual({ 2: [0] })

    // ...until the entry expires, after which it routes to the leader again and the entry is gone.
    group.preferredReadReplicasPerTopicPartition[TOPIC][0].expireAt = Date.now() - 1
    expect(group.findReadReplicaForPartitions(TOPIC, [0])).toEqual({ [NODE_ID]: [0] })
    expect(group.preferredReadReplicasPerTopicPartition[TOPIC][0]).toBeUndefined()
  })

  test('abortedTransactions are filtered out of batch.messages under read_committed', async () => {
    const { group, setResponse } = await createFetchGroup({ partitions: [0] })
    group.isolationLevel = 1 // READ_COMMITTED

    // One aborted transactional batch (producerId 1000, firstOffset 100) plus one committed
    // record after it, so the filter has something to keep as well as something to drop.
    setResponse(
      fetchResponse([
        {
          partition: 0,
          errorCode: 0,
          records: ABORTED_AND_COMMITTED_BATCHES,
          abortedTransactions: [{ producerId: '1000', firstOffset: '100' }],
        },
      ])
    )
    const [batch] = await group.fetch(NODE_ID)

    expect(batch.rawMessages.length).toBeGreaterThan(batch.messages.length)
    // Named, not counted: `every` is true on an empty array, so a filter that dropped the
    // committed record too would pass a length-only assertion.
    expect(batch.messages.map(({ offset }) => offset)).toEqual(['101'])
  })
})

describe('Consumer > a moved leader reaches its new node within the idle and busy bounds', () => {
  const MAX_WAIT = 400
  const ERROR_AT = 100
  const POLL_DONE_AT = 350
  const ROUND = 10
  const HINT = { leaderId: 1, leaderEpoch: 9 }
  const ENDPOINTS = [{ nodeId: 1, host: 'kafka1', port: 9093, rack: null }]

  /** @returns {Promise<void>} lets continuations and real immediates run, with no virtual time */
  const drain = async () => {
    for (let round = 0; round < 8; round++) {
      await new Promise(setImmediate)
      await jest.advanceTimersByTimeAsync(0)
    }
  }

  /** @returns {{promise: Promise<void>, release: () => void}} a request held open from outside */
  const hold = () => {
    const held = {}
    held.promise = new Promise((resolve) => {
      held.release = resolve
    })
    return held
  }

  /**
   * Moves partition 0 from node 0 to node 1 under a running fetch manager and records, in virtual
   * time, when node 0 answered with the error, when node 1's outstanding long poll completed, and
   * when node 1 first asked for the moved partition. Node 0's Fetch and, when busy, node 1's long
   * poll are held until the case releases them; every other Fetch is answered at once.
   * @param {{hinted: boolean, busy: boolean}} arm whether the old leader names the new one, and
   *   whether node 1 is in a long poll for another partition when the leader moves
   * @returns {Promise<object>} the timeline and the counts the arm asserts
   */
  const moveLeader = async ({ hinted, busy }) => {
    const h = await createFetchGroup({
      partitions: busy ? [0, 1] : [0],
      partitionLeaders: busy ? { 0: 0, 1: 1 } : { 0: 0 },
      knownNodes: [0, 1],
      applyLeaderHintMovesLeaderTo: 1,
    })
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] })
    h.group.maxWaitTime = MAX_WAIT
    if (!hinted)
      h.cluster.refreshMetadata.mockImplementation(async () => h.installRouting({ 0: 1 }))

    const real = h.broker.fetch.bind(h.broker)
    const frames = []
    const holds = new Map([['0|0', hold()], ...(busy ? [['1|1', hold()]] : [])])
    const errorBody = fetchResponse(
      [
        {
          partition: 0,
          errorCode: 6,
          ...(hinted && { currentLeader: HINT }),
        },
      ],
      { ...(hinted && { nodeEndpoints: ENDPOINTS }) }
    )
    h.cluster.findBroker = async ({ nodeId }) =>
      Object.assign(Object.create(h.broker), {
        fetch: async (request) => {
          const partitions = request.topics.flatMap((t) => t.partitions.map((p) => p.partition))
          const key = `${nodeId}|${partitions.join()}`
          const frame = { nodeId, partitions, held: holds.has(key) }
          frames.push(frame)
          if (frame.held) await holds.get(key).promise
          frame.at = Date.now()
          h.setResponse(
            key === '0|0'
              ? errorBody
              : fetchResponse(partitions.map((partition) => ({ partition, errorCode: 0 })))
          )
          const answer = await real(request)
          frame.doneAt = Date.now()
          return answer
        },
      })

    const manager = createFetchManager({
      logger: newLogger(),
      getNodeIds: () => [0, 1],
      fetch: (nodeId) => h.group.fetch(nodeId),
      handler: async () => {},
    })
    const started = manager.start()
    try {
      await drain()
      await jest.advanceTimersByTimeAsync(ERROR_AT)
      holds.get('0|0').release()
      await drain()
      if (busy) {
        await jest.advanceTimersByTimeAsync(POLL_DONE_AT - ERROR_AT)
        holds.get('1|1').release()
        await drain()
      }
      return {
        errorAt: frames.find(({ held, nodeId }) => held && nodeId === 0).doneAt,
        pollDoneAt: busy ? frames.find(({ held, nodeId }) => held && nodeId === 1).doneAt : null,
        moved: frames.find(
          ({ nodeId, partitions }) =>
            nodeId === 1 &&
            partitions.includes(0) &&
            !(partitions.length === 1 && partitions[0] === 1)
        ),
        hints: h.cluster.applyLeaderHint.mock.calls.length,
        refreshes: h.cluster.refreshMetadata.mock.calls.length,
        rejoins: h.requestRejoin.mock.calls.length,
        fetchers: manager.getFetchers().length,
        frames,
      }
    } finally {
      h.group.onStopRequested()
      await drain()
      await jest.advanceTimersByTimeAsync(MAX_WAIT + ROUND)
      await manager.stop()
      await started.catch(() => {})
      holds.forEach(({ release }) => release())
      jest.clearAllTimers()
      jest.useRealTimers()
      await h.broker.disconnect()
    }
  }

  it.each([
    ['hinted', true],
    ['unhinted', false],
  ])('%s: an idle new node asks for the partition in its next round', async (_name, hinted) => {
    const timeline = await moveLeader({ hinted, busy: false })

    expect(timeline.moved).toBeDefined()
    expect(timeline.moved.at - timeline.errorAt).toBeLessThanOrEqual(ROUND)
    expect(timeline.hints).toBe(hinted ? 1 : 0)
    expect(timeline.refreshes).toBe(hinted ? 0 : 1)
    expect(timeline.rejoins).toBe(0)
    expect(timeline.fetchers).toBe(2)
  })

  it.each([
    ['hinted', true],
    ['unhinted', false],
  ])(
    '%s: a busy new node asks when its long poll completes, never cancelling it',
    async (_name, hinted) => {
      const timeline = await moveLeader({ hinted, busy: true })

      expect(timeline.moved).toBeDefined()
      expect(timeline.moved.at).toBeGreaterThanOrEqual(timeline.pollDoneAt)
      expect(timeline.moved.at - timeline.pollDoneAt).toBeLessThanOrEqual(ROUND)
      expect(timeline.moved.at - timeline.errorAt).toBeLessThanOrEqual(MAX_WAIT + ROUND)
      expect(timeline.frames.filter(({ nodeId, held }) => nodeId === 1 && held)).toHaveLength(1)
      expect(timeline.hints).toBe(hinted ? 1 : 0)
      expect(timeline.refreshes).toBe(hinted ? 0 : 1)
      expect(timeline.rejoins).toBe(0)
      expect(timeline.fetchers).toBe(2)
    }
  )
})
