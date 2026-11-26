const apiKeys = require('../../protocol/requests/apiKeys')
const { decode } = require('../../protocol/requests/offsetForLeaderEpoch/v4/response')
const {
  NODE_ID,
  TOPIC,
  apiVersionsBody,
  createFakeBroker,
  createFetchGroup,
  offsetForLeaderEpochResponse,
  scriptedEpochLog,
} = require('./fixtures/fetchHarness')

/**
 * The broker's answer to OffsetForLeaderEpoch comes from `LeaderEpochFileCache#endOffsetFor`
 * (storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java).
 * When it finds nothing (end offset -1), `UnifiedLog#endOffsetForEpoch` returns no value
 * (core/.../log/UnifiedLog.scala 4.0.0:944-951; storage/.../log/UnifiedLog.java 4.4.0:1316-1325)
 * and `Partition#lastOffsetForLeaderEpoch` sets only the partition and code NONE
 * (core/.../cluster/Partition.scala 4.0.0:1729-1731, 4.4.0:1598-1600), so the row carries the
 * message defaults: LeaderEpoch -1, EndOffset -1 (OffsetForLeaderEpochResponse.json).
 */
const CACHE = {
  epochs: [
    [3, 20],
    [5, 40],
    [8, 100],
  ],
  logEnd: 150,
}
const UNDEFINED = { leaderEpoch: -1, endOffset: '-1' }

/**
 * A request for one topic, in the shape the consumer group sends.
 * @param {number} partition
 * @param {number} leaderEpoch the epoch asked about
 * @returns {{ topics: object[] }}
 */
const validationRequest = (partition, leaderEpoch) => ({
  topics: [{ topic: TOPIC, partitions: [{ partition, currentLeaderEpoch: 9, leaderEpoch }] }],
})

describe('Test harness > scripted OffsetForLeaderEpoch answers', () => {
  // Source lines are LeaderEpochFileCache.java at 4.0.0 / 4.4.0.
  test.each([
    // the latest cached epoch: that epoch and the log end (4.0.0:293-298, 4.4.0:292-297)
    ['the current epoch', 8, CACHE, { leaderEpoch: 8, endOffset: '150' }],
    // same branch for a leader that wrote nothing in its epoch yet (start = log end)
    [
      'the current epoch before any record of it',
      8,
      {
        epochs: [
          [3, 20],
          [8, 150],
        ],
        logEnd: 150,
      },
      { leaderEpoch: 8, endOffset: '150' },
    ],
    // floor and next start (4.0.0:300,306,314-317; 4.4.0:299,305,313-316)
    [
      'an epoch present in the cache below the current one',
      5,
      CACHE,
      { leaderEpoch: 5, endOffset: '100' },
    ],
    ['an epoch between two cached epochs', 6, CACHE, { leaderEpoch: 5, endOffset: '100' }],
    // no floor: the requested epoch and the first cached start (4.0.0:307-313, 4.4.0:306-312)
    ['an epoch below the first cached one', 2, CACHE, { leaderEpoch: 2, endOffset: '20' }],
    // no higher entry: undefined (4.0.0:300-304, 4.4.0:299-303)
    ['an epoch above the current one', 9, CACHE, UNDEFINED],
    // no latest epoch and no higher entry: undefined (4.0.0:293,300-304; 4.4.0:292,299-303)
    ['any epoch of an empty cache', 5, { epochs: [], logEnd: 0 }, UNDEFINED],
    // the undefined epoch itself (4.0.0:289-292, 4.4.0:288-291)
    ['the undefined epoch', -1, CACHE, UNDEFINED],
  ])('scriptedEpochLog answers %s as the broker does', (_case, requested, log, expected) => {
    expect(scriptedEpochLog(log).endOffsetFor(requested)).toEqual(expected)
  })

  test('scriptedEpochLog writes one row per requested partition, each for the epoch it asked about', async () => {
    const body = scriptedEpochLog(CACHE).answer([0, 1, 2], {
      nodeId: NODE_ID,
      partitions: [
        { partition: 0, currentLeaderEpoch: 8, leaderEpoch: 5 },
        { partition: 1, currentLeaderEpoch: 8, leaderEpoch: 2 },
        { partition: 2, currentLeaderEpoch: 8, leaderEpoch: 9 },
      ],
    })

    expect((await decode(body)).topics).toEqual([
      {
        topic: TOPIC,
        partitions: [
          { errorCode: 0, partition: 0, leaderEpoch: 5, endOffset: '100' },
          { errorCode: 0, partition: 1, leaderEpoch: 2, endOffset: '20' },
          { errorCode: 0, partition: 2, leaderEpoch: -1, endOffset: '-1' },
        ],
      },
    ])
  })

  test.each([
    [
      'epochs that do not rise',
      [
        [5, 0],
        [5, 10],
      ],
      20,
    ],
    [
      'starts that fall',
      [
        [3, 10],
        [5, 0],
      ],
      20,
    ],
    [
      'a start past the log end',
      [
        [3, 0],
        [5, 30],
      ],
      20,
    ],
  ])('scriptedEpochLog refuses a cache with %s', (_case, epochs, logEnd) => {
    expect(() => scriptedEpochLog({ epochs, logEnd })).toThrow(/epoch 5/)
  })
})

describe('Test harness > withheld broker answers', () => {
  test('a withheld answer lets a later answer on the same connection through, and arrives when released', async () => {
    let release
    const withheld = new Promise((resolve) => (release = resolve))
    let validations = 0
    const { broker } = createFakeBroker((header) => {
      if (header.apiKey === apiKeys.ApiVersions) return apiVersionsBody(18)
      validations += 1
      return validations === 1
        ? withheld
        : offsetForLeaderEpochResponse([{ partition: 1, leaderEpoch: 4, endOffset: '30' }])
    })
    await broker.connect()

    const first = broker.offsetForLeaderEpoch(validationRequest(0, 4))
    const firstSettled = jest.fn()
    first.then(firstSettled)

    const second = await broker.offsetForLeaderEpoch(validationRequest(1, 4))
    expect(second.topics[0].partitions).toEqual([
      { errorCode: 0, partition: 1, leaderEpoch: 4, endOffset: '30' },
    ])
    expect(validations).toBe(2)
    expect(firstSettled).not.toHaveBeenCalled()

    release(offsetForLeaderEpochResponse([{ partition: 0, leaderEpoch: 4, endOffset: '20' }]))
    expect((await first).topics[0].partitions).toEqual([
      { errorCode: 0, partition: 0, leaderEpoch: 4, endOffset: '20' },
    ])
    await broker.disconnect()
  })
})

describe('Test harness > fetch group routing', () => {
  test('by default the group sees a constant leader epoch and a forget only records the call', async () => {
    const { cluster, installRouting } = await createFetchGroup({ knownNodes: [0, 1] })

    installRouting({ 0: 1 })
    expect(cluster.leaderEpoch(TOPIC, 0)).toBe(5)
    expect(cluster.cachedLeader(TOPIC, 0)).toBe(1)
    expect(cluster.forgetLeaderEpoch(TOPIC, 0)).toBe(true)
    expect(cluster.leaderEpoch(TOPIC, 0)).toBe(5)
  })

  test('with liveRouting the group sees the epoch the metadata cache holds, and a forget clears it there', async () => {
    const { cluster, installRouting, metadataCache } = await createFetchGroup({
      knownNodes: [0, 1],
      liveRouting: true,
    })
    expect(cluster.leaderEpoch(TOPIC, 0)).toBe(5)

    installRouting({ 0: 1 })
    expect(cluster.leaderEpoch(TOPIC, 0)).toBe(6)
    expect(cluster.cachedLeader(TOPIC, 0)).toBe(1)

    await cluster.applyLeaderHint({ topic: TOPIC, partition: 0, leaderId: 0, leaderEpoch: 9 })
    expect(cluster.leaderEpoch(TOPIC, 0)).toBe(9)
    expect(cluster.cachedLeader(TOPIC, 0)).toBe(0)

    expect(cluster.forgetLeaderEpoch(TOPIC, 0)).toBe(true)
    expect(cluster.forgetLeaderEpoch).toHaveBeenCalledWith(TOPIC, 0)
    expect(cluster.leaderEpoch(TOPIC, 0)).toBe(-1)
    expect(metadataCache.leaderEpoch(TOPIC, 0)).toBe(-1)
  })

  test('with validationBrokers each node answers on its own connection and is told which node it is', async () => {
    const { cluster, setValidationResponse, validationRequestsTo, writesTo } =
      await createFetchGroup({ partitions: [0, 1], knownNodes: [0, 1], validationBrokers: true })
    const asked = []
    setValidationResponse((partitions, { nodeId, partitions: rows }) => {
      asked.push({ nodeId, partitions, epochs: rows.map(({ leaderEpoch }) => leaderEpoch) })
      return offsetForLeaderEpochResponse(partitions.map((partition) => ({ partition })))
    })

    const node0 = await cluster.findBroker({ nodeId: 0 })
    const node1 = await cluster.findBroker({ nodeId: 1 })
    expect(node1).not.toBe(node0)
    expect(await cluster.findBroker({ nodeId: 1 })).toBe(node1)

    await node1.offsetForLeaderEpoch(validationRequest(1, 3))

    expect(asked).toEqual([{ nodeId: 1, partitions: [1], epochs: [3] }])
    expect(validationRequestsTo(1)).toEqual([
      {
        replicaId: -1,
        topics: [
          { topic: TOPIC, partitions: [{ partition: 1, currentLeaderEpoch: 9, leaderEpoch: 3 }] },
        ],
      },
    ])
    expect(validationRequestsTo(0)).toEqual([])
    expect(writesTo(2)).toEqual([])
  })

  test('a held broker lookup waits until released while other nodes are found', async () => {
    const { cluster, holdBrokerLookup } = await createFetchGroup({
      knownNodes: [0, 1],
      validationBrokers: true,
    })
    const release = holdBrokerLookup(1)
    const found = jest.fn()
    const held = cluster.findBroker({ nodeId: 1 }).then(found)

    await cluster.findBroker({ nodeId: 0 })
    await new Promise(setImmediate)
    expect(found).not.toHaveBeenCalled()

    release()
    await held
    expect(found).toHaveBeenCalledTimes(1)
    expect(found.mock.calls[0][0]).not.toBe(await cluster.findBroker({ nodeId: 0 }))
  })
})
