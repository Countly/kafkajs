const Broker = require('../../broker')
const Connection = require('../../network/connection')
const Decoder = require('../../protocol/decoder')
const Encoder = require('../../protocol/encoder')
const ConsumerGroup = require('../consumerGroup')
const OffsetManager = require('../offsetManager')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const apiKeys = require('../../protocol/requests/apiKeys')
const { responseHeaderVersion } = require('../../protocol/headers')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const {
  newLogger,
  createFakeSocketFactory,
  respondWithFrame,
  readRequestHeader,
} = require('testHelpers')

const NODE_ID = 0
const TOPIC = 'test-topic'
const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'

/**
 * One real record batch, built by the production encoder so that `decodeMessages` reads it the
 * way it reads a broker's. A stand-in buffer would fail the magic-byte read inside the decoder.
 * @type {Buffer}
 */
let RECORD_BATCH

beforeAll(async () => {
  RECORD_BATCH = Buffer.from(
    (
      await RecordBatch({
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: [RecordEncoder({ key: Buffer.from('key'), value: Buffer.from('value') })],
      })
    ).buffer
  )
})

/** A v3/v4-shaped ApiVersions body advertising one Fetch range. */
const apiVersionsBody = (fetchMaxVersion) =>
  new Encoder()
    .writeInt16(0)
    .writeUVarIntArray([
      new Encoder()
        .writeInt16(apiKeys.Fetch)
        .writeInt16(4)
        .writeInt16(fetchMaxVersion)
        .writeTaggedFields(),
    ])
    .writeInt32(0)
    .writeTaggedFields().buffer

/**
 * The leader-hint tag body carried on a fetch response: a compact array of NodeId/Host/Port/Rack
 * structs.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimisations+for+the+client
 * @param {{ nodeId: number, host: string, port: number, rack: string }[]} endpoints
 * @returns {Buffer}
 */
const encodeNodeEndpoints = (endpoints) =>
  new Encoder().writeUVarIntArray(
    endpoints.map(({ nodeId, host, port, rack }) =>
      new Encoder()
        .writeInt32(nodeId)
        .writeUVarIntString(host)
        .writeInt32(port)
        .writeUVarIntString(rack)
        .writeTaggedFields()
    )
  ).buffer

/**
 * A Fetch v18 response body: one topic keyed by id, one entry per partition, each with its own
 * error code and optionally a leader hint; an optional top-level error code and endpoint hint.
 *
 * @param {{ partition: number, errorCode: number, records?: boolean, preferredReadReplica?: number,
 *   currentLeader?: { leaderId: number, leaderEpoch: number } }[]} partitions
 * @param {{ topLevel?: number, nodeEndpoints?: object[] }} [options]
 * @returns {Buffer}
 */
const fetchResponse = (partitions, { topLevel = 0, nodeEndpoints } = {}) =>
  new Encoder()
    .writeInt32(0) // clientSideThrottleTime
    .writeInt16(topLevel)
    .writeInt32(0) // sessionId
    .writeUVarIntArray([
      new Encoder()
        .writeUuid(TOPIC_ID)
        .writeUVarIntArray(
          partitions.map(
            ({ partition, errorCode, records = false, preferredReadReplica = -1, currentLeader }) =>
              new Encoder()
                .writeInt32(partition)
                .writeInt16(errorCode)
                .writeInt64(100) // highWatermark
                .writeInt64(100) // lastStableOffset
                .writeInt64(0) // logStartOffset
                .writeUVarIntArray([]) // abortedTransactions
                .writeInt32(preferredReadReplica)
                .writeUVarInt(records ? RECORD_BATCH.length + 1 : 0)
                .writeBuffer(records ? RECORD_BATCH : Buffer.alloc(0))
                .writeTaggedFields(
                  currentLeader
                    ? [
                        {
                          tag: 1,
                          value: new Encoder()
                            .writeInt32(currentLeader.leaderId)
                            .writeInt32(currentLeader.leaderEpoch).buffer,
                        },
                      ]
                    : undefined
                )
          )
        )
        .writeTaggedFields(),
    ])
    .writeTaggedFields(
      nodeEndpoints ? [{ tag: 0, value: encodeNodeEndpoints(nodeEndpoints) }] : undefined
    ).buffer

/**
 * Decodes the **last** Fetch request written to the fake socket.
 *
 * The version is read off the request header, because the leading scalars differ by it:
 * `ReplicaId` exists from v0 to v14 and is gone from v15, and topics are keyed by name up to v12
 * and by id from v13. Everything else is identical across v12-v18.
 *
 * @param {Buffer[]} writes `state.writes`
 * @returns {{ apiVersion: number, topics: {topic: string|null, topicId: string|null,
 *   partitions: {partition: number, currentLeaderEpoch: number, fetchOffset: string,
 *   lastFetchedEpoch: number, logStartOffset: string, maxBytes: number}[]}[] }}
 */
const decodeFetchRequest = (writes) => {
  const frame = [...writes].reverse().find((w) => readRequestHeader(w).apiKey === apiKeys.Fetch)
  const { apiVersion } = readRequestHeader(frame)

  // size(4) apiKey(2) apiVersion(2) correlationId(4) clientId then a tag buffer - request header
  // v2, whose clientId stays an INT16-prefixed string (see src/protocol/request.js).
  const decoder = new Decoder(frame)
  decoder.readInt32()
  decoder.readInt16()
  decoder.readInt16()
  decoder.readInt32()
  decoder.readString()
  decoder.readTaggedFields()

  const replicaId = apiVersion <= 14 ? decoder.readInt32() : null
  const maxWaitMs = decoder.readInt32()
  const minBytes = decoder.readInt32()
  const maxBytes = decoder.readInt32()
  const isolationLevel = decoder.readInt8()
  const sessionId = decoder.readInt32()
  const sessionEpoch = decoder.readInt32()

  const topics = decoder.readUVarIntArray((d) => {
    const topic = apiVersion <= 12 ? d.readUVarIntString() : null
    const topicId = apiVersion >= 13 ? d.readUuid() : null
    const partitions = d.readUVarIntArray((p) => {
      const entry = {
        partition: p.readInt32(),
        currentLeaderEpoch: p.readInt32(),
        fetchOffset: p.readInt64().toString(),
        lastFetchedEpoch: p.readInt32(),
        logStartOffset: p.readInt64().toString(),
        maxBytes: p.readInt32(),
      }
      p.readTaggedFields()
      return entry
    })
    d.readTaggedFields()
    return { topic, topicId, partitions }
  })

  // forgottenTopicsData: this client always sends it empty, so the element reader is never called
  // and the decode stays in step.
  decoder.readUVarIntArray(() => {})
  const rackId = decoder.readUVarIntString()
  decoder.readTaggedFields()

  return {
    apiVersion,
    replicaId,
    maxWaitMs,
    minBytes,
    maxBytes,
    isolationLevel,
    sessionId,
    sessionEpoch,
    topics,
    rackId,
  }
}

/**
 * Builds a real `Broker` wired to a real `Connection` over the fake socket from `testHelpers`.
 * `answer` receives each outgoing request's header and returns the response body to play back;
 * each response is framed with the header version that request's api key and version imply.
 * @param {(header: { apiKey: number, apiVersion: number }) => Buffer} answer
 * @returns {{ broker: Broker, state: { writes: Buffer[] } }}
 */
const createFakeBroker = (answer) => {
  const { factory, state } = createFakeSocketFactory()
  const connection = new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    clientId: 'test-client',
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
  })

  let answered = 0
  const pump = async () => {
    while (answered < state.writes.length) {
      const header = readRequestHeader(state.writes[answered])
      answered += 1
      respondWithFrame(
        state,
        header.correlationId,
        responseHeaderVersion(header.apiKey, header.apiVersion),
        answer(header)
      )
    }
  }

  const connectionPool = {
    host: '127.0.0.1',
    port: 9092,
    sasl: null,
    generation: 0,
    connectionTimeout: 1000,
    isConnected: () => connection.isConnected(),
    isAuthenticated: () => true,
    getConnection: async () => {
      await connection.connect()
      return connection
    },
    getConnectionByRequest: async () => {
      await connection.connect()
      return connection
    },
    send: async (protocolRequest) => {
      const promise = connection.send(protocolRequest)
      await new Promise(setImmediate)
      await pump()
      return promise
    },
    setVersions: () => {},
    destroy: async () => connection.disconnect(),
  }

  return { broker: new Broker({ connectionPool, logger: newLogger() }), state }
}

/**
 * Builds a `ConsumerGroup` whose join/sync dance is skipped: `subscriptionState` and
 * `offsetManager` are primed directly, mirroring what a real join would have produced.
 * @param {{ cluster: object, broker: Broker, partitions: number[] }} options
 * @returns {ConsumerGroup}
 */
const buildConsumerGroup = ({ cluster, broker, partitions }) => {
  const group = new ConsumerGroup({
    retry: { retries: 0 },
    cluster,
    groupId: 'test-group',
    topics: [TOPIC],
    topicConfigurations: { [TOPIC]: {} },
    logger: newLogger(),
    instrumentationEmitter: new InstrumentationEventEmitter(),
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 1048576,
    maxWaitTimeInMs: 100,
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    isolationLevel: 0,
    rackId: '',
    checkCrcs: true,
    metadataMaxAge: 300000,
  })

  group.subscriptionState.assign([{ topic: TOPIC, partitions }])
  group.offsetManager = new OffsetManager({
    cluster,
    coordinator: broker,
    memberAssignment: { [TOPIC]: partitions },
    autoCommit: false,
    topicConfigurations: { [TOPIC]: {} },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'test-group',
    generationId: 0,
    memberId: 'member-1',
  })

  // A real join would have resolved these from OffsetFetch; priming them directly is what lets
  // this scenario skip the join dance while keeping the Fetch path completely real.
  for (const partition of partitions) {
    group.offsetManager.committedOffsets()[TOPIC][partition] = '0'
  }

  return group
}

/**
 * A primed `ConsumerGroup` over a real `Broker`/`Connection`/fake socket, whose Fetch responses
 * are **scripted per round**: `setResponse(body)` decides what the next fetch answers with, so
 * one case can drive several rounds with different bodies.
 *
 * `joinAndSync` is stubbed (and returned) because the contract under test is that a partition
 * error never reaches it; leaving the real one in place would attempt a real join.
 *
 * @param {{ partitions?: number[], knownNodes?: number[] }} [options]
 */
const createFetchGroup = async ({ partitions = [0], knownNodes = [NODE_ID] } = {}) => {
  let nextResponse = fetchResponse(partitions.map((partition) => ({ partition, errorCode: 0 })))
  const setResponse = (body) => {
    nextResponse = body
  }

  const { broker, state } = createFakeBroker((header) =>
    header.apiKey === apiKeys.ApiVersions ? apiVersionsBody(18) : nextResponse
  )
  await broker.connect()

  const findBrokerCalls = []
  const cluster = {
    refreshMetadataIfNecessary: async () => {},
    refreshMetadata: jest.fn(async () => {}),
    findBroker: async (args) => {
      findBrokerCalls.push(args)
      return broker
    },
    findTopicPartitionMetadata: () =>
      partitions.map((partitionId) => ({ partitionId, leader: NODE_ID })),
    committedOffsets: () => ({}),
    topicId: () => TOPIC_ID,
    leaderEpoch: () => 5,
    forgetLeaderEpoch: jest.fn(() => true),
    applyLeaderHint: jest.fn(async () => true),
    hasNode: jest.fn((nodeId) => knownNodes.includes(Number(nodeId))),
  }

  const group = buildConsumerGroup({ cluster, broker, partitions })
  const joinAndSync = jest.spyOn(group, 'joinAndSync').mockResolvedValue(undefined)

  return {
    group,
    cluster,
    logger: group.logger,
    offsetManager: group.offsetManager,
    joinAndSync,
    setResponse,
    findBrokerCalls,
    /** The per-partition fields of the last Fetch frame actually written to the socket. */
    lastFetchRequest: () => decodeFetchRequest(state.writes).topics[0].partitions,
  }
}

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
})
