const createFetchManager = require('../fetchManager')
const { newLogger } = require('testHelpers')
const {
  NODE_ID,
  TOPIC,
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
    const { group, cluster, joinAndSync, setResponse } = await createFetchGroup({ partitions: [0] })

    setResponse(
      fetchResponse(
        [{ partition: 0, errorCode: 6, currentLeader: { leaderId: 2, leaderEpoch: 9 } }],
        {
          nodeEndpoints: [{ nodeId: 2, host: 'kafka2', port: 9095, rack: null }],
        }
      )
    )
    await group.fetch(NODE_ID)

    expect(cluster.applyLeaderHint).toHaveBeenCalledWith({
      topic: TOPIC,
      partition: 0,
      leaderId: 2,
      leaderEpoch: 9,
      nodeEndpoints: [{ nodeId: 2, host: 'kafka2', port: 9095, rack: null }],
    })
    // A usable hint is the refresh: nothing else is needed this round.
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(joinAndSync).not.toHaveBeenCalled()
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
      expect.objectContaining({ leaderId: 2, leaderEpoch: 9, nodeEndpoints: undefined })
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
      expect.objectContaining({ leaderId: 2, nodeEndpoints: otherNodeEndpoints })
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
      expect.objectContaining({ leaderId: 2, leaderEpoch: 9, nodeEndpoints: undefined })
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

    expect(findBrokerCalls).toEqual([{ nodeId: NODE_ID }, { nodeId: 2 }])
  })
})

describe('Consumer > a hint that adds an unknown node rebalances the fetchers, not the group', () => {
  test('every fetcher is recreated and no batch is lost across it', async () => {
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
    await new Promise((resolve) => setTimeout(resolve, 200))
    await fetchManager.stop()
    await started.catch(() => {})

    // The whole fetcher set was torn down and recreated: the new node is fetched from, and there
    // is now one fetcher per node. Both are observations of the manager, not of the stub that
    // published the node - without the rebalance the run ends with one fetcher, still fetching
    // only node 0, and the partitions that moved are silently never fetched again.
    expect([...fetchedNodes].sort()).toEqual([0, 2])
    expect(fetchManager.getFetchers()).toHaveLength(2)

    // The batch fetched in the round that triggered the rebalance was still handed to the worker
    // queue rather than dropped on the teardown.
    expect(delivered).toContain('a')
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

  test('checkCrcs still throws KafkaJSPartialMessageError with crcMismatch on a corrupted batch', async () => {
    const { group, setResponse } = await createFetchGroup({ partitions: [0], checkCrcs: true })

    setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: CORRUPTED_RECORD_BATCH }]))

    await expect(group.fetch(NODE_ID)).rejects.toMatchObject({
      name: 'KafkaJSPartialMessageError',
      crcMismatch: true,
      retriable: false,
    })
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
    expect(batch.messages.every(({ offset }) => Number(offset) >= 101)).toBe(true)
  })
})
