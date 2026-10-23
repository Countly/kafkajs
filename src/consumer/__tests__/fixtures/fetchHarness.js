const Broker = require('../../../broker')
const Connection = require('../../../network/connection')
const Decoder = require('../../../protocol/decoder')
const Encoder = require('../../../protocol/encoder')
const ConsumerGroup = require('../../consumerGroup')
const OffsetManager = require('../../offsetManager')
const { MemberAssignment } = require('../../assignerProtocol')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const apiKeys = require('../../../protocol/requests/apiKeys')
const { responseHeaderVersion } = require('../../../protocol/headers')
const { RecordBatch } = require('../../../protocol/recordBatch/v0')
const RecordEncoder = require('../../../protocol/recordBatch/record/v0')
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

/**
 * Builds one record batch with the production encoder and returns its bytes.
 * @param {object} [options] passed straight to `RecordBatch`
 * @returns {Promise<Buffer>}
 */
const buildRecordBatch = async (options = {}) =>
  Buffer.from(
    (
      await RecordBatch({
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: [RecordEncoder({ key: Buffer.from('key'), value: Buffer.from('value') })],
        ...options,
      })
    ).buffer
  )

/**
 * Builds the record-batch fixtures every fetch response in these specs plays back, and caches the
 * plain one so `fetchResponse({ records: true })` can reach it. Call once from `beforeAll`: the
 * production encoder is asynchronous, so the buffers cannot be built at module load.
 *
 * @returns {Promise<{ RECORD_BATCH: Buffer, CORRUPTED_RECORD_BATCH: Buffer,
 *   ABORTED_AND_COMMITTED_BATCHES: Buffer }>} the plain batch; the same batch with one byte flipped
 *   inside its CRC-covered region; and two concatenated batches, an aborted transactional one at
 *   offset 100 followed by a committed one at offset 101
 */
const prepareRecordBatches = async () => {
  RECORD_BATCH = await buildRecordBatch()

  const corrupted = Buffer.from(RECORD_BATCH)
  // FirstOffset(8) + Length(4) + PartitionLeaderEpoch(4) + Magic(1) + CRC(4) + Attributes(2) +
  // LastOffsetDelta(4) + FirstTimestamp(8) + MaxTimestamp(8) + ProducerId(8): the next byte
  // (ProducerEpoch) is inside the covered region but is plain carried-through metadata.
  const PRODUCER_EPOCH_OFFSET = 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8
  corrupted[PRODUCER_EPOCH_OFFSET] ^= 0x01

  const abortedBatch = await buildRecordBatch({
    firstOffset: '100',
    transactional: true,
    producerId: '1000',
  })
  const committedBatch = await buildRecordBatch({ firstOffset: '101' })

  return {
    RECORD_BATCH,
    CORRUPTED_RECORD_BATCH: corrupted,
    ABORTED_AND_COMMITTED_BATCHES: Buffer.concat([abortedBatch, committedBatch]),
  }
}

/**
 * A v3/v4-shaped ApiVersions body advertising one Fetch range and, optionally, the
 * OffsetForLeaderEpoch range this client implements. Omitting the latter is how a broker that
 * cannot serve position validation is modelled: the version is decided from this table, never
 * from a thrown error.
 * @param {number} fetchMaxVersion
 * @param {boolean} [offsetForLeaderEpoch] whether api key 23 appears at all
 * @returns {Buffer}
 */
const apiVersionsBody = (fetchMaxVersion, offsetForLeaderEpoch = true) => {
  const apis = [
    new Encoder()
      .writeInt16(apiKeys.Fetch)
      .writeInt16(4)
      .writeInt16(fetchMaxVersion)
      .writeTaggedFields(),
  ]

  if (offsetForLeaderEpoch) {
    apis.push(
      new Encoder()
        .writeInt16(apiKeys.OffsetForLeaderEpoch)
        .writeInt16(0)
        .writeInt16(4)
        .writeTaggedFields()
    )
  }

  return new Encoder().writeInt16(0).writeUVarIntArray(apis).writeInt32(0).writeTaggedFields()
    .buffer
}

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
 * The compact record set of one partition entry.
 * @param {boolean|Buffer} records `true` for the shared batch, `false` for the compact null, or
 *   the exact bytes to play back
 * @returns {(encoder: Encoder) => Encoder}
 */
const encodeRecords = (records) => {
  if (records === false) return (e) => e.writeUVarInt(0)
  const buffer = records === true ? RECORD_BATCH : records
  return (e) => e.writeUVarInt(buffer.length + 1).writeBuffer(buffer)
}

/**
 * @param {{ producerId: string, firstOffset: string }[]} [aborted]
 * @returns {Encoder[]}
 */
const encodeAbortedTransactions = (aborted = []) =>
  aborted.map(({ producerId, firstOffset }) =>
    new Encoder().writeInt64(producerId).writeInt64(firstOffset).writeTaggedFields()
  )

/**
 * A flexible Fetch response body: one topic, one entry per partition, each with its own error
 * code and optionally a leader hint; an optional top-level error code and endpoint hint.
 *
 * The topic key follows the version: a name up to v12, a topic id from v13. `NodeEndpoints` is a
 * v16+ field, so a body written at a lower version simply carries no tag 0 - which is what makes
 * the endpoint-less rule testable against a real v12 frame rather than by equivalence.
 *
 * @param {{ partition: number, errorCode: number, records?: boolean|Buffer,
 *   preferredReadReplica?: number, abortedTransactions?: object[],
 *   currentLeader?: { leaderId: number, leaderEpoch: number } }[]} partitions
 * @param {{ topLevel?: number, nodeEndpoints?: object[], apiVersion?: number }} [options]
 * @returns {Buffer}
 */
const fetchResponse = (partitions, { topLevel = 0, nodeEndpoints, apiVersion = 18 } = {}) =>
  new Encoder()
    .writeInt32(0) // clientSideThrottleTime
    .writeInt16(topLevel)
    .writeInt32(0) // sessionId
    .writeUVarIntArray([
      (apiVersion <= 12
        ? new Encoder().writeUVarIntString(TOPIC)
        : new Encoder().writeUuid(TOPIC_ID)
      )
        .writeUVarIntArray(
          partitions.map(
            ({
              partition,
              errorCode,
              records = false,
              preferredReadReplica = -1,
              abortedTransactions,
              currentLeader,
            }) =>
              encodeRecords(records)(
                new Encoder()
                  .writeInt32(partition)
                  .writeInt16(errorCode)
                  .writeInt64(100) // highWatermark
                  .writeInt64(100) // lastStableOffset
                  .writeInt64(0) // logStartOffset
                  .writeUVarIntArray(encodeAbortedTransactions(abortedTransactions))
                  .writeInt32(preferredReadReplica)
              ).writeTaggedFields(
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
 * @returns {{ apiVersion: number, replicaId: number|null, maxWaitMs: number, minBytes: number,
 *   maxBytes: number, isolationLevel: number, sessionId: number, sessionEpoch: number,
 *   rackId: string|null, topics: {topic: string|null, topicId: string|null,
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
 * An OffsetForLeaderEpoch v4 response body: one topic, one entry per partition.
 *
 * @see clients/src/main/resources/common/message/OffsetForLeaderEpochResponse.json (Apache Kafka 4.3.1)
 * @param {{ partition: number, errorCode?: number, leaderEpoch?: number, endOffset?: string }[]} partitions
 * @returns {Buffer}
 */
const offsetForLeaderEpochResponse = (partitions) =>
  new Encoder()
    .writeInt32(0) // throttleTimeMs
    .writeUVarIntArray([
      new Encoder()
        .writeUVarIntString(TOPIC)
        .writeUVarIntArray(
          partitions.map(({ partition, errorCode = 0, leaderEpoch = 4, endOffset = '20' }) =>
            new Encoder()
              .writeInt16(errorCode)
              .writeInt32(partition)
              .writeInt32(leaderEpoch)
              .writeInt64(endOffset)
              .writeTaggedFields()
          )
        )
        .writeTaggedFields(),
    ])
    .writeTaggedFields().buffer

/**
 * Decodes every OffsetForLeaderEpoch frame written to the fake socket, oldest first, so a spec can
 * assert both how many attempts went out and which partitions each one carried.
 *
 * @see clients/src/main/resources/common/message/OffsetForLeaderEpochRequest.json (Apache Kafka 4.3.1)
 * @param {Buffer[]} writes `state.writes`
 * @returns {{ replicaId: number, topics: { topic: string, partitions: { partition: number,
 *   currentLeaderEpoch: number, leaderEpoch: number }[] }[] }[]}
 */
const decodeOffsetForLeaderEpochRequests = (writes) =>
  writes
    .filter((w) => readRequestHeader(w).apiKey === apiKeys.OffsetForLeaderEpoch)
    .map((frame) => {
      // size(4) apiKey(2) apiVersion(2) correlationId(4) clientId then a tag buffer - request
      // header v2, whose clientId stays an INT16-prefixed string (see src/protocol/request.js).
      const decoder = new Decoder(frame)
      decoder.readInt32()
      decoder.readInt16()
      decoder.readInt16()
      decoder.readInt32()
      decoder.readString()
      decoder.readTaggedFields()

      const replicaId = decoder.readInt32()
      const topics = decoder.readUVarIntArray((d) => {
        const topic = d.readUVarIntString()
        const partitions = d.readUVarIntArray((p) => {
          const entry = {
            partition: p.readInt32(),
            currentLeaderEpoch: p.readInt32(),
            leaderEpoch: p.readInt32(),
          }
          p.readTaggedFields()
          return entry
        })
        d.readTaggedFields()
        return { topic, partitions }
      })

      return { replicaId, topics }
    })

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
    throwIfRetired: () => {},
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
 * The ApiVersions table the membership harness pins, so version negotiation is a constant of this
 * file rather than whatever a broker happens to advertise. The maxima are the versions this repo
 * implements for the four classic group requests.
 * @see https://kafka.apache.org/43/design/protocol
 * @returns {Buffer}
 */
const membershipApiVersionsBody = () => {
  const api = (apiKey, minVersion, maxVersion) =>
    new Encoder()
      .writeInt16(apiKey)
      .writeInt16(minVersion)
      .writeInt16(maxVersion)
      .writeTaggedFields()

  return new Encoder()
    .writeInt16(0)
    .writeUVarIntArray([
      api(apiKeys.JoinGroup, 0, 5),
      api(apiKeys.SyncGroup, 0, 3),
      api(apiKeys.Heartbeat, 0, 3),
      api(apiKeys.LeaveGroup, 0, 3),
    ])
    .writeInt32(0)
    .writeTaggedFields().buffer
}

/**
 * A real `Broker` over a fake socket, pinned to the membership ApiVersions table above and
 * answering JoinGroup/SyncGroup/Heartbeat/LeaveGroup with the bodies `answer` returns.
 *
 * `bodyOf(apiKey)` returns the LAST request written for that api key with its request header
 * stripped, which is what a golden frame comparison needs: the correlation id and the client id
 * live in the header and move with call order, while the body is the thing the codec produced.
 *
 * @param {(header: { apiKey: number, apiVersion: number }) => Buffer} answer
 * @returns {Promise<{ broker: Broker, state: { writes: Buffer[] },
 *   bodyOf: (apiKey: number) => { apiKey: number, apiVersion: number, body: Buffer } }>}
 * @throws {Error} from `bodyOf` when no request was written for the requested api key
 */
const createFakeCoordinator = async (answer) => {
  const { broker, state } = createFakeBroker((header) =>
    header.apiKey === apiKeys.ApiVersions ? membershipApiVersionsBody() : answer(header)
  )

  await broker.connect()

  const bodyOf = (apiKey) => {
    for (let i = state.writes.length - 1; i >= 0; i--) {
      const write = state.writes[i]
      const header = readRequestHeader(write)
      if (header.apiKey !== apiKey) {
        continue
      }

      return {
        apiKey: header.apiKey,
        apiVersion: header.apiVersion,
        body: write.subarray(header.byteLength),
      }
    }

    throw new Error(`no request written for apiKey ${apiKey}`)
  }

  return { broker, state, bodyOf }
}

/**
 * Builds a `ConsumerGroup` whose join/sync dance is skipped: `subscriptionState` and
 * `offsetManager` are primed directly, mirroring what a real join would have produced.
 * @param {{ cluster: object, broker: Broker, partitions: number[], checkCrcs: boolean,
 *   maxDecompressedBatchBytes?: number, autoCommit: boolean, retry: object, position: string }} options
 * @returns {ConsumerGroup}
 */
const buildConsumerGroup = ({
  cluster,
  broker,
  partitions,
  checkCrcs,
  maxDecompressedBatchBytes,
  autoCommit,
  retry,
  position,
}) => {
  const group = new ConsumerGroup({
    retry,
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
    autoCommit,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    isolationLevel: 0,
    rackId: '',
    checkCrcs,
    maxDecompressedBatchBytes,
    metadataMaxAge: 300000,
  })

  group.subscriptionState.assign([{ topic: TOPIC, partitions }])
  group.assignmentEpoch += 1
  for (const partition of partitions) {
    const acquiredAt = ++group.acquireClock
    group.acquired.set(`${TOPIC}|${partition}`, { acquiredAt, changedAt: acquiredAt })
  }
  group.offsetManager = new OffsetManager({
    cluster,
    coordinator: broker,
    memberAssignment: { [TOPIC]: partitions },
    autoCommit,
    topicConfigurations: { [TOPIC]: {} },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'test-group',
    generationId: 0,
    memberId: 'member-1',
  })

  // A real join would have resolved these from OffsetFetch; priming them directly is what lets
  // this scenario skip the join dance while keeping the Fetch path completely real.
  for (const partition of partitions) {
    group.offsetManager.committedOffsets()[TOPIC][partition] = position
  }

  return group
}

/**
 * A primed `ConsumerGroup` over a real `Broker`/`Connection`/fake socket, whose Fetch responses
 * are **scripted per round**: `setResponse(body)` decides what the next fetch answers with, so
 * one case can drive several rounds with different bodies. A function may be passed instead of a
 * buffer, and is called at the moment the fake broker answers the frame - which is the window
 * between the request being built and the response being walked.
 *
 * The same broker answers OffsetForLeaderEpoch from the validation options below, or from the
 * function `setValidationResponse` installs, which receives the partitions the frame asked for.
 *
 * `joinAndSync` is stubbed (and returned) because the contract under test is that a partition
 * error never reaches it; leaving the real one in place would attempt a real join. `sync()` still
 * drives the real sequence, against the stubbed coordinator.
 *
 * @param {object} [options]
 * @param {number[]} [options.partitions] the assigned partitions, all led by `NODE_ID` initially
 * @param {number[]} [options.knownNodes] node ids `cluster.hasNode` answers `true` for
 * @param {boolean} [options.checkCrcs]
 * @param {number} [options.maxDecompressedBatchBytes] the group's decompression bound
 * @param {number} [options.fetchMaxVersion] the highest Fetch version the fake broker advertises,
 *   which is the version the client then negotiates
 * @param {boolean} [options.applyLeaderHintResolves] what `cluster.applyLeaderHint` resolves to;
 *   `false` is how the cache rejects a hint that does not advance the partition's leader epoch
 * @param {number} [options.applyLeaderHintMovesLeaderTo] when set, an applied leader hint also
 *   rewrites the hinted partition's leader in the metadata the cluster answers with, so the next
 *   round's routing follows the hint the way it would against a real cluster
 * @param {boolean} [options.autoCommit] the group's and the offset manager's auto-commit setting
 * @param {object} [options.retry] the group's own retry options
 * @param {string} [options.position] the committed offset every partition starts from
 * @param {number} [options.committedEpoch] the tracked leader epoch every partition starts with;
 *   `-1`, the default, means "unknown", which is what keeps a spec that is not about validation
 *   free of OffsetForLeaderEpoch traffic
 * @param {number} [options.leaderEpoch] what `cluster.leaderEpoch` answers
 * @param {string} [options.endOffset] the `EndOffset` the OffsetForLeaderEpoch reply carries
 * @param {number} [options.validationErrorCode] the per-partition code that reply carries
 * @param {boolean} [options.validationThrows] make the OffsetForLeaderEpoch answer throw
 * @param {boolean} [options.advertiseOffsetForLeaderEpoch] whether ApiVersions lists api key 23
 * @param {number[]} [options.withholdValidationFor] partitions the reply leaves out entirely
 * @param {number} [options.preferredReadReplica] seeds every partition's preferred read replica
 * @param {Record<number, number>} [options.partitionLeaders] the leader of each partition; every
 *   partition is led by `NODE_ID` unless this says otherwise
 * @returns {Promise<{ group: ConsumerGroup, cluster: object, broker: Broker, logger: object,
 *   offsetManager: OffsetManager, offsetCommit: jest.SpyInstance, joinAndSync: jest.SpyInstance,
 *   requestRejoin: jest.Mock,
 *   setResponse: (body: Buffer|(() => Buffer)) => void,
 *   setValidationResponse: (fn: (partitions: number[]) => Buffer) => void,
 *   findBrokerCalls: {nodeId: number}[], sync: () => Promise<void>,
 *   requests: () => object[], validationRequests: () => object[],
 *   fetchRequestPartitions: () => number[], fetchRequestOffsets: () => string[],
 *   lastFetchRequest: () => object[],
 *   lastFetchApiVersion: () => number, lastFetchHeaderFields: () => object }>}
 */
const createFetchGroup = async ({
  partitions = [0],
  knownNodes = [NODE_ID],
  checkCrcs = true,
  maxDecompressedBatchBytes,
  fetchMaxVersion = 18,
  applyLeaderHintResolves = true,
  applyLeaderHintMovesLeaderTo,
  autoCommit = false,
  retry = { retries: 0 },
  position = '0',
  committedEpoch = -1,
  leaderEpoch = 5,
  endOffset = '20',
  validationErrorCode = 0,
  validationThrows = false,
  advertiseOffsetForLeaderEpoch = true,
  withholdValidationFor = [],
  preferredReadReplica,
  partitionLeaders,
} = {}) => {
  let nextResponse = fetchResponse(
    partitions.map((partition) => ({ partition, errorCode: 0 })),
    { apiVersion: fetchMaxVersion }
  )
  const setResponse = (body) => {
    nextResponse = body
  }

  let validationAnswer = (requestedPartitions) => {
    if (validationThrows) {
      throw new Error('connection lost')
    }

    return offsetForLeaderEpochResponse(
      requestedPartitions
        .filter((partition) => !withholdValidationFor.includes(partition))
        .map((partition) => ({
          partition,
          errorCode: validationErrorCode,
          leaderEpoch: 4,
          endOffset,
        }))
    )
  }
  const setValidationResponse = (fn) => {
    validationAnswer = fn
  }

  const { broker, state } = createFakeBroker((header) => {
    if (header.apiKey === apiKeys.ApiVersions) {
      return apiVersionsBody(fetchMaxVersion, advertiseOffsetForLeaderEpoch)
    }

    if (header.apiKey === apiKeys.OffsetForLeaderEpoch) {
      const sent = decodeOffsetForLeaderEpochRequests(state.writes)
      const requested = sent[sent.length - 1].topics[0].partitions.map(({ partition }) => partition)
      return validationAnswer(requested)
    }

    return typeof nextResponse === 'function' ? nextResponse() : nextResponse
  })
  await broker.connect()

  const leaders = new Map(
    partitions.map((partition) => [
      partition,
      partitionLeaders ? partitionLeaders[partition] : NODE_ID,
    ])
  )
  const findBrokerCalls = []
  const coordinator = {
    isConnected: () => true,
    joinGroup: async () => ({
      generationId: 1,
      leaderId: 'leader-1',
      memberId: 'member-1',
      members: [],
      groupProtocol: 'RoundRobinAssigner',
    }),
    syncGroup: async () => ({
      memberAssignment: MemberAssignment.encode({
        version: 1,
        assignment: { [TOPIC]: partitions },
      }),
    }),
    offsetCommit: async () => ({}),
  }
  const cluster = {
    refreshMetadataIfNecessary: async () => {},
    refreshMetadata: jest.fn(async () => {}),
    findBroker: async (args) => {
      findBrokerCalls.push(args)
      return broker
    },
    findGroupCoordinator: async () => coordinator,
    findTopicPartitionMetadata: () =>
      partitions.map((partitionId) => ({ partitionId, leader: leaders.get(partitionId) })),
    findLeaderForPartitions: (topic, partitionIds) =>
      partitionIds.reduce((result, id) => {
        const leader = leaders.get(Number(id))
        return { ...result, [leader]: [...(result[leader] || []), Number(id)] }
      }, {}),
    committedOffsets: () => ({}),
    defaultOffset: () => '-2',
    topicId: () => TOPIC_ID,
    leaderEpoch: () => leaderEpoch,
    forgetLeaderEpoch: jest.fn(() => true),
    applyLeaderHint: jest.fn(async ({ partition }) => {
      if (applyLeaderHintResolves && applyLeaderHintMovesLeaderTo != null) {
        leaders.set(partition, applyLeaderHintMovesLeaderTo)
      }
      return applyLeaderHintResolves
    }),
    hasNode: jest.fn((nodeId) => knownNodes.includes(Number(nodeId))),
  }

  const group = buildConsumerGroup({
    cluster,
    broker,
    partitions,
    checkCrcs,
    maxDecompressedBatchBytes,
    autoCommit,
    retry,
    position,
  })

  for (const partition of partitions) {
    group.offsetManager.resolvedEpochs[TOPIC][partition] = committedEpoch
  }

  if (preferredReadReplica != null) {
    group.preferredReadReplicasPerTopicPartition[TOPIC] = Object.fromEntries(
      partitions.map((partition) => [
        partition,
        { nodeId: preferredReadReplica, expireAt: Date.now() + 60000 },
      ])
    )
  }

  // Every offset commit the group makes in these specs (an auto-commit seek, a reset) would go to
  // the fake broker, whose version table advertises no OffsetCommit; the coordinator round trip is
  // not what any of them is about.
  const offsetCommit = jest.spyOn(broker, 'offsetCommit').mockResolvedValue({ topics: [] })

  // Captured before the stub so `sync()` can drive the production join/sync against the stubbed
  // coordinator, while the fetch path keeps seeing a rejoin it must never trigger.
  const realJoinAndSync = group.joinAndSync.bind(group)
  const joinAndSync = jest.spyOn(group, 'joinAndSync').mockResolvedValue(undefined)

  // The observable for "a fetch error never rejoins": `recoverFromFetch` asks the coordinator,
  // it does not call `joinAndSync` any more. `heartbeatManager` is deliberately null - the
  // join bracket is guarded on its presence and these groups never join.
  const requestRejoin = jest.fn()
  group.attach({ rebalanceCoordinator: { requestRejoin }, heartbeatManager: null })

  return {
    group,
    cluster,
    broker,
    logger: group.logger,
    offsetManager: group.offsetManager,
    offsetCommit,
    joinAndSync,
    requestRejoin,
    setResponse,
    setValidationResponse,
    findBrokerCalls,
    /** Drives the real join/sync sequence, and the apply that used to live inside it. */
    sync: async () => {
      const result = await realJoinAndSync()
      return group.applyRebalanceResult(result)
    },
    /** The header of every request written to the socket, oldest first. */
    requests: () => state.writes.map(readRequestHeader),
    /** Every OffsetForLeaderEpoch frame written to the socket, decoded, oldest first. */
    validationRequests: () => decodeOffsetForLeaderEpochRequests(state.writes),
    /** The partitions of the last Fetch frame on the wire. */
    fetchRequestPartitions: () =>
      decodeFetchRequest(state.writes).topics[0].partitions.map(({ partition }) => partition),
    /** The fetch offsets of the last Fetch frame on the wire. */
    fetchRequestOffsets: () =>
      decodeFetchRequest(state.writes).topics[0].partitions.map(({ fetchOffset }) => fetchOffset),
    /** The per-partition fields of the last Fetch frame actually written to the socket. */
    lastFetchRequest: () => decodeFetchRequest(state.writes).topics[0].partitions,
    /** The version the last Fetch frame was actually encoded at. */
    lastFetchApiVersion: () => decodeFetchRequest(state.writes).apiVersion,
    /** The leading scalars and the trailing rackId of the last Fetch frame. */
    lastFetchHeaderFields: () => {
      const {
        replicaId,
        maxWaitMs,
        minBytes,
        maxBytes,
        isolationLevel,
        sessionId,
        sessionEpoch,
        rackId,
      } = decodeFetchRequest(state.writes)
      return {
        replicaId,
        maxWaitMs,
        minBytes,
        maxBytes,
        isolationLevel,
        sessionId,
        sessionEpoch,
        rackId,
      }
    },
  }
}

module.exports = {
  NODE_ID,
  TOPIC,
  TOPIC_ID,
  prepareRecordBatches,
  apiVersionsBody,
  decodeFetchRequest,
  decodeOffsetForLeaderEpochRequests,
  createFakeBroker,
  createFakeCoordinator,
  fetchResponse,
  offsetForLeaderEpochResponse,
  createFetchGroup,
}
