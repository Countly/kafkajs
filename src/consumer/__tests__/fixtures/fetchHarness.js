const Broker = require('../../../broker')
const setupPool = require('../../../../testHelpers/setupPool')
const Connection = require('../../../network/connection')
const Decoder = require('../../../protocol/decoder')
const Encoder = require('../../../protocol/encoder')
const ConsumerGroup = require('../../consumerGroup')
const Cluster = require('../../../cluster')
const MetadataCache = require('../../../cluster/metadataCache')
const OffsetManager = require('../../offsetManager')
const { MemberAssignment } = require('../../assignerProtocol')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const apiKeys = require('../../../protocol/requests/apiKeys')
const { errorCodes } = require('../../../protocol/error')
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
 * @param {{ topLevel?: number, nodeEndpoints?: object[], apiVersion?: number,
 *   sessionId?: number, includeTopic?: boolean, topicId?: string }} [options] `topicId` is the
 *   topic's id from v13, the harness's own by default
 * @returns {Buffer}
 */
const fetchResponse = (
  partitions,
  {
    topLevel = 0,
    nodeEndpoints,
    apiVersion = 18,
    sessionId = 0,
    includeTopic = true,
    topicId = TOPIC_ID,
  } = {}
) =>
  new Encoder()
    .writeInt32(0) // clientSideThrottleTime
    .writeInt16(topLevel)
    .writeInt32(sessionId)
    .writeUVarIntArray(
      includeTopic
        ? [
            (apiVersion <= 12
              ? new Encoder().writeUVarIntString(TOPIC)
              : new Encoder().writeUuid(topicId)
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
          ]
        : []
    )
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
 *   rackId: string|null, forgottenTopics: {topic: string|null, topicId: string|null,
 *   partitions: number[]}[], topics: {topic: string|null, topicId: string|null,
 *   partitions: {partition: number, currentLeaderEpoch: number, fetchOffset: string,
 *   lastFetchedEpoch: number, logStartOffset: string, maxBytes: number}[]}[] }}
 */
const decodeFetchRequest = (writes) => {
  const frame = [...writes].reverse().find((w) => readRequestHeader(w).apiKey === apiKeys.Fetch)
  const { apiVersion } = readRequestHeader(frame)
  if (apiVersion < 12 || apiVersion > 18) {
    throw new Error(`Fetch request reader does not support v${apiVersion}`)
  }

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

  const forgottenTopics = decoder.readUVarIntArray((d) => {
    const topic = apiVersion <= 12 ? d.readUVarIntString() : null
    const topicId = apiVersion >= 13 ? d.readUuid() : null
    const partitions = d.readUVarIntArray((p) => p.readInt32())
    d.readTaggedFields()
    return { topic, topicId, partitions }
  })
  const rackId = decoder.readUVarIntString()
  decoder.readTaggedFields()
  if (decoder.canReadBytes(1)) {
    throw new Error(`Fetch v${apiVersion} request has a byte after its tag buffer`)
  }

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
    forgottenTopics,
    rackId,
  }
}

const INT32_MIN = -(2 ** 31)
const INT32_MAX = 2 ** 31 - 1
const INT64_MIN = -(2n ** 63n)
const INT64_MAX = 2n ** 63n - 1n

/**
 * @param {number} value
 * @param {string} name what the value is, for the error
 * @param {number} min
 * @param {number} [max] the int32 maximum by default
 * @returns {number} `value`
 * @throws {Error} unless `value` is an integer from `min` to `max`
 */
const integer = (value, name, min, max = INT32_MAX) => {
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(`${name} ${value} is not an integer from ${min} to ${max}`)
  }
  return value
}

/**
 * An int64 given as a safe integer, a decimal string or a bigint, kept exact.
 * @param {number|string|bigint} value
 * @param {string} name what the value is, for the error
 * @param {bigint} min
 * @returns {bigint} the exact value
 * @throws {Error} unless `value` is exactly an integer from `min` to the int64 maximum
 */
const int64 = (value, name, min) => {
  let exact = null
  if (typeof value === 'bigint') exact = value
  else if (typeof value === 'string' && /^-?\d+$/.test(value)) exact = BigInt(value)
  else if (Number.isSafeInteger(value)) exact = BigInt(value)
  if (exact === null || exact < min || exact > INT64_MAX) {
    throw new Error(`${name} ${String(value)} is not an exact int64 from ${min} to ${INT64_MAX}`)
  }
  return exact
}

/**
 * An OffsetForLeaderEpoch v4 response body exactly as scripted: one topic, one entry per
 * partition, any value the field types hold (int16 code, int32 partition and `LeaderEpoch`, int64
 * `EndOffset`) and no broker rule. Only for a test that sends a malformed answer on purpose to
 * exercise the client's refusal of it; a broker's answer is `offsetForLeaderEpochResponse`.
 *
 * @see clients/src/main/resources/common/message/OffsetForLeaderEpochResponse.json (Apache Kafka 4.3.1)
 * @param {{ partition: number, errorCode: number, leaderEpoch: number,
 *   endOffset: number|string|bigint }[]} partitions
 * @returns {Buffer}
 * @throws {Error} for a value the field's type cannot hold
 */
const offsetForLeaderEpochRawResponse = (partitions) =>
  new Encoder()
    .writeInt32(0) // throttleTimeMs
    .writeUVarIntArray([
      new Encoder()
        .writeUVarIntString(TOPIC)
        .writeUVarIntArray(
          partitions.map(({ partition, errorCode, leaderEpoch, endOffset }) =>
            new Encoder()
              .writeInt16(integer(errorCode, 'error code', -(2 ** 15), 2 ** 15 - 1))
              .writeInt32(integer(partition, 'partition', INT32_MIN))
              .writeInt32(integer(leaderEpoch, 'leader epoch', INT32_MIN))
              .writeInt64(int64(endOffset, 'end offset', INT64_MIN).toString())
              .writeTaggedFields()
          )
        )
        .writeTaggedFields(),
    ])
    .writeTaggedFields().buffer

/**
 * One row as a broker answers it, with the defaults filled in.
 * @param {{ partition: number, errorCode?: number, leaderEpoch?: number,
 *   endOffset?: number|string|bigint }} row
 * @returns {{ partition: number, errorCode: number, leaderEpoch: number,
 *   endOffset: number|string|bigint }}
 * @throws {Error} for a row no broker sends
 */
const brokerRow = ({ partition, errorCode = 0, ...fields }) => {
  integer(partition, 'partition', 0)
  integer(errorCode, 'error code', -1, 2 ** 15 - 1)
  const failed = errorCode !== 0
  const { leaderEpoch = failed ? -1 : 4, endOffset = failed ? '-1' : '20' } = fields
  integer(leaderEpoch, 'leader epoch', -1)
  const undefinedEnd = int64(endOffset, 'end offset', -1n) === -1n
  if (failed ? leaderEpoch !== -1 || !undefinedEnd : (leaderEpoch === -1) !== undefinedEnd) {
    throw new Error(
      `partition ${partition}: error ${errorCode} with LeaderEpoch ${leaderEpoch} and EndOffset ` +
        `${endOffset} is no broker answer (an error or an undefined answer is -1 with -1)`
    )
  }
  return { partition, errorCode, leaderEpoch, endOffset }
}

/**
 * An OffsetForLeaderEpoch v4 response body as a broker sends it: one topic, one entry per
 * partition. Apache Kafka 4.0.0 and 4.4.0 answer a found epoch with its end offset (both 0 or
 * more), an undefined lookup with `LeaderEpoch` -1 and `EndOffset` -1, and an error with the
 * partition and code alone, so both fields keep their -1 defaults (`Partition#
 * lastOffsetForLeaderEpoch`, `UnifiedLog#endOffsetForEpoch`, `KafkaApis`). A row with a non-zero
 * code therefore defaults to -1 and -1, a successful one to epoch 4 and end 20.
 *
 * @see clients/src/main/resources/common/message/OffsetForLeaderEpochResponse.json (Apache Kafka 4.3.1)
 * @param {{ partition: number, errorCode?: number, leaderEpoch?: number,
 *   endOffset?: number|string|bigint }[]} partitions
 * @returns {Buffer}
 * @throws {Error} for a field value or an epoch and end pair no broker sends
 */
const offsetForLeaderEpochResponse = (partitions) =>
  offsetForLeaderEpochRawResponse(partitions.map(brokerRow))

/**
 * @param {string} type an error type of `src/protocol/error.js`
 * @returns {number} its code
 */
const codeOf = (type) => errorCodes.find((error) => error.type === type).code

/**
 * One broker's replica of a partition: its epoch cache, log end, leader epoch and whether it
 * leads, answering OffsetForLeaderEpoch as Apache Kafka 4.0.0 and 4.4.0 do. Per row, in order:
 *
 * 1. a `CurrentLeaderEpoch` of -1 skips the epoch check (`ReplicaManager#lastOffsetForLeaderEpoch`);
 *    any other is compared with the broker's leader epoch: below it `FENCED_LEADER_EPOCH`, above
 *    it `UNKNOWN_LEADER_EPOCH` (`Partition#checkCurrentLeaderEpoch`);
 * 2. a broker that does not lead answers `NOT_LEADER_OR_FOLLOWER` (`Partition#getLocalLog`);
 * 3. the leader answers code 0 from `LeaderEpochFileCache#endOffsetFor`:
 *    - the requested epoch is -1, or the cache is empty, or the epoch is above the latest: -1 / -1;
 *    - the latest epoch: that epoch and the log end;
 *    - a lower epoch: the largest cached epoch at or below it and the start of the next cached one;
 *    - below the first cached epoch: the requested epoch itself and the first cached start.
 *
 * An error row, like an empty lookup, carries only partition and code, so the message defaults
 * (`LeaderEpoch` -1, `EndOffset` -1) apply. A broker that does not host the partition is not
 * modelled.
 *
 * The cache must be one a broker can hold: every assignment removes the entries at or above the
 * new epoch or start, and refuses a negative epoch or start (`LeaderEpochFileCache#assign`), so
 * epochs and starts both rise strictly. A leader assigns its epoch at its log end on election, so
 * its leader epoch is its latest cached one; no replica's is below that. Epochs are int32 and
 * offsets int64 (the response's `LeaderEpoch` and `EndOffset`), both 0 or more; an offset beyond
 * the safe integer range is given as a string or bigint and kept exact.
 *
 * @param {{ epochs: [number, number|string|bigint][], logEnd: number|string|bigint,
 *   leader?: boolean, leaderEpoch?: number }} log `[epoch, start]` pairs, the log end offset, whether this broker
 *   leads (default true), and its leader epoch (default the latest cached epoch; required to
 *   answer a current epoch from an empty cache)
 * @returns {{ endOffsetFor: (requestedEpoch: number) => { leaderEpoch: number, endOffset: string },
 *   answer: (partitions: number[], request: { partitions: { partition: number,
 *     currentLeaderEpoch: number, leaderEpoch: number }[] }) => Buffer }} `endOffsetFor` is the
 *   cache lookup alone; `answer` fits `setValidationResponse`
 * @throws {Error} for a state no broker could hold, and from `answer` for a row without a
 *   current epoch or one it cannot check
 */
const scriptedEpochLog = ({ epochs, logEnd, leader = true, leaderEpoch }) => {
  const cache = epochs.map(([epoch, start]) => ({
    epoch: integer(epoch, 'cached epoch', 0),
    start: int64(start, `epoch ${epoch} start`, 0n),
  }))
  const end = int64(logEnd, 'log end', 0n)
  cache.forEach(({ epoch, start }, i) => {
    const previous = cache[i - 1]
    if (previous && (epoch <= previous.epoch || start <= previous.start)) {
      throw new Error(`epoch ${epoch} at ${start} does not follow epoch ${previous.epoch}`)
    }
    if (start > end) throw new Error(`epoch ${epoch} starts at ${start}, past the log end ${end}`)
  })
  const latest = cache[cache.length - 1]
  if (leaderEpoch !== undefined) integer(leaderEpoch, 'leader epoch', 0)
  if (
    leaderEpoch !== undefined &&
    ((latest && leaderEpoch < latest.epoch) || (leader && latest && leaderEpoch !== latest.epoch))
  ) {
    throw new Error(
      `leader epoch ${leaderEpoch} cannot go with a latest cached epoch ${latest?.epoch}`
    )
  }
  const brokerEpoch = leaderEpoch ?? latest?.epoch

  const undefinedAnswer = { leaderEpoch: -1, endOffset: '-1' }
  const endOffsetFor = (requestedEpoch) => {
    integer(requestedEpoch, 'requested epoch', INT32_MIN)
    if (requestedEpoch === -1) return undefinedAnswer
    if (latest && latest.epoch === requestedEpoch) {
      return { leaderEpoch: requestedEpoch, endOffset: end.toString() }
    }

    const higher = cache.find(({ epoch }) => epoch > requestedEpoch)
    if (!higher) return undefinedAnswer
    const floor = cache.findLast(({ epoch }) => epoch <= requestedEpoch)
    return {
      leaderEpoch: floor ? floor.epoch : requestedEpoch,
      endOffset: higher.start.toString(),
    }
  }

  const rowFor = ({ partition, currentLeaderEpoch, leaderEpoch: requested }) => {
    const error = (type) => ({ partition, errorCode: codeOf(type), ...undefinedAnswer })
    if (!Number.isInteger(currentLeaderEpoch)) {
      throw new Error(`partition ${partition} carries no current leader epoch`)
    }
    if (currentLeaderEpoch !== -1) {
      if (brokerEpoch === undefined) {
        throw new Error('an empty cache names no leader epoch to check against: pass leaderEpoch')
      }
      if (brokerEpoch > currentLeaderEpoch) return error('FENCED_LEADER_EPOCH')
      if (brokerEpoch < currentLeaderEpoch) return error('UNKNOWN_LEADER_EPOCH')
    }
    // Code 6, Kafka's NOT_LEADER_OR_FOLLOWER.
    if (!leader) return error('NOT_LEADER_FOR_PARTITION')
    if (requested < -1) {
      throw new Error(
        `partition ${partition} asks for epoch ${requested}, which no client sends: script the ` +
          'answer to that malformed request with offsetForLeaderEpochRawResponse'
      )
    }
    return { partition, errorCode: 0, ...endOffsetFor(requested) }
  }

  return {
    endOffsetFor,
    answer: (_partitions, { partitions }) => offsetForLeaderEpochResponse(partitions.map(rowFor)),
  }
}

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
 * `answer` receives each outgoing request's header and the exact frame it answers, and returns the
 * response body to play back; each response carries that request's correlation id, is framed with
 * the header version its api key and version imply, and goes to the socket the frame was written
 * to, never to another: a destroyed socket gets nothing.
 *
 * Playback is driven by the writes themselves: each frame written to a socket is answered one
 * `setImmediate` later, in write order, whoever wrote it. A frame is owned by its socket and the
 * correlation id in its header, and nothing else.
 *
 * The fake broker owns every playback it schedules. `close()` cancels the ones still queued and
 * stops it answering: a frame written afterwards is recorded and never answered, and a withheld
 * answer settling afterwards neither answers nor fails its request, which stays in flight until
 * the connection closes. Close it before the test ends, so no answer runs after its test.
 *
 * A Buffer is played back at once. A thenable is a withheld answer: it is played back when it
 * resolves, and later requests are answered meanwhile. A broker answers one connection in order;
 * overtaking here stands in for the separate connection the real pool gives Fetch. A thenable
 * created before `answer` returns it must come from `withheldAnswer()`, which is handled from
 * creation, so rejecting it early is never an unhandled rejection.
 *
 * A playback failure (`answer` throwing, a withheld answer rejecting, or the playback of an answer
 * throwing, a `chunks` assertion say) fails exactly the request the frame belongs to, with the
 * error itself, through the connection's own in-flight entry: the entry leaves the queue as an
 * answered one does, so the connection stays usable and an ordinary `disconnect()` drains. A
 * failure after its request has settled (a withheld answer rejected after a close) changes nothing.
 * `broker.disconnect({ drainTimeout: 0 })` closes at once and fails every request still waiting.
 * Setup gets two minutes to tolerate coverage runner delays; Jest still bounds a hung test.
 * @param {(header: { apiKey: number, apiVersion: number, correlationId: number },
 *   frame: Buffer) => Buffer|PromiseLike<Buffer>} answer
 * @param {(frame: Buffer, header: object) => Buffer[]} [chunks] complete-frame transport chunks
 * @returns {{ broker: Broker, state: { writes: Buffer[] }, close: () => void }}
 */
const createFakeBroker = (answer, chunks) => {
  const { factory: socketFactory, state } = createFakeSocketFactory()
  /** @type {Set<NodeJS.Immediate>} playbacks scheduled and not yet run */
  const playbacks = new Set()
  let closed = false
  /**
   * The fake socket factory, with every recorded write answered one `setImmediate` later while
   * the fake broker is open.
   * @param {object} options what `Connection` passes the socket factory
   * @returns {object} the fake socket
   */
  const factory = (options) => {
    const socket = socketFactory(options)
    const write = socket.write
    socket.write = (...args) => {
      const written = write(...args)
      if (!closed) {
        const frame = socket.writes[socket.writes.length - 1]
        const playback = setImmediate(() => {
          playbacks.delete(playback)
          playBack(socket, frame)
        })
        playbacks.add(playback)
      }
      return written
    }
    return socket
  }
  const connection = new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    clientId: 'test-client',
    connectionTimeout: 120000,
    requestTimeout: 1000,
    connectionsMaxIdle: 0,
    enforceRequestTimeout: false,
  })

  /**
   * Plays one response back through `target.socket`, split by `chunks` when given.
   * @param {{ socket: object }} target
   * @param {{ apiKey: number, apiVersion: number, correlationId: number }} header
   * @param {Buffer} body
   * @returns {void}
   */
  const reply = (target, header, body) => {
    const responseState = chunks
      ? {
          socket: {
            emit: (_event, frame) => {
              for (const chunk of chunks(frame, header)) target.socket.emit('data', chunk)
            },
          },
        }
      : target
    respondWithFrame(
      responseState,
      header.correlationId,
      responseHeaderVersion(header.apiKey, header.apiVersion),
      body
    )
  }

  /**
   * Fails one in-flight request with `error` the way the queue settles an answered one: the entry
   * leaves the in-flight set, pending requests may go out, the request settles, and the queue's
   * observers (the drain of `disconnect()` among them) hear of it. A request no longer in flight on
   * `socket`'s generation (answered, or failed by a close) is left alone.
   * @param {object} socket the socket the request's frame was written to
   * @param {number} correlationId
   * @param {Error} error
   * @returns {void}
   */
  const failRequest = (socket, correlationId, error) => {
    const queue = connection.requestQueue
    const request = queue.inflight.get(correlationId)
    if (connection.socket !== socket || !request) return
    queue.inflight.delete(correlationId)
    try {
      queue.checkPendingRequests()
    } finally {
      request.rejected(error)
    }
    queue.emitQueueState()
  }

  /**
   * Answers one written frame on its socket; a failure fails that frame's request alone.
   * @param {object} socket
   * @param {Buffer} frame
   * @returns {void}
   */
  const playBack = (socket, frame) => {
    const header = readRequestHeader(frame)
    const fail = (error) => {
      if (!closed) failRequest(socket, header.correlationId, error)
    }
    const deliver = (body) => {
      if (!closed && !socket.destroyed) reply({ socket }, header, body)
    }
    try {
      const body = answer(header, frame)
      if (typeof body?.then === 'function') {
        Promise.resolve(body).then(deliver).catch(fail)
      } else {
        deliver(body)
      }
    } catch (error) {
      fail(error)
    }
  }

  const connectionPool = setupPool({
    host: '127.0.0.1',
    port: 9092,
    sasl: null,
    generation: 0,
    connectionTimeout: 120000,
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
    // As the real pool does, a request's pre-write guard reaches the connection, which asks it
    // again at the instant of transmission.
    send: (protocolRequest, _connection, options) =>
      connection.send(
        options?.guard ? { ...protocolRequest, guard: options.guard } : protocolRequest
      ),
    setVersions: () => {},
    destroy: async (options) => connection.disconnect(options),
  })

  /**
   * Stops answering: cancels every queued playback, and nothing written or settled later answers.
   * @returns {void}
   */
  const close = () => {
    closed = true
    for (const playback of playbacks) clearImmediate(playback)
    playbacks.clear()
  }

  return { broker: new Broker({ connectionPool, logger: newLogger() }), state, close }
}

/**
 * A withheld answer a spec holds before the fake broker asks for it: return `promise` from an
 * answer callback, then `resolve(body)` to play the body back or `reject(error)` to fail the
 * request it answers with `error`. The promise is handled from creation, so a rejection that comes
 * before the fake broker takes it is never unhandled; a hold no answer ever returns answers nothing.
 * @returns {{ promise: Promise<Buffer>, resolve: (body: Buffer) => void,
 *   reject: (error: Error) => void }}
 */
const withheldAnswer = () => {
  let resolve
  let reject
  const promise = new Promise((settle, fail) => {
    resolve = settle
    reject = fail
  })
  promise.catch(() => {})
  return { promise, resolve, reject }
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
  groupProtocol,
  fetchSessions,
  requestTimeout,
  rackId,
}) => {
  const group = new ConsumerGroup({
    groupProtocol,
    fetchSessions,
    requestTimeout,
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
    rackId,
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
 * function `setValidationResponse` installs. That function receives the partitions the frame asked
 * for, then `{ nodeId, partitions }`: the node answering and the decoded rows of the very frame it
 * answers, each with its `currentLeaderEpoch` and requested `leaderEpoch`. It may return a promise
 * to withhold the answer (see `createFakeBroker`); `scriptedEpochLog(...).answer` fits it, one log
 * per node when nodes hold different states.
 *
 * `holdBrokerLookup(nodeId)` makes every `cluster.findBroker` for that node wait until the
 * returned release function is called, or until `teardown()`, which rejects it.
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
 * @param {number} [options.leaderEpoch] what `cluster.leaderEpoch` answers, and the epoch the
 *   metadata cache starts at
 * @param {boolean} [options.liveRouting] when true, the metadata cache is the one routing source:
 *   `cluster.leaderEpoch` and `cluster.onPartitionRouted` (the `Cluster` methods),
 *   `findLeaderForPartitions` and `findTopicPartitionMetadata` read it, so `installRouting` and an
 *   accepted leader hint move the leader and epoch the group sees and end its admissions;
 *   `cluster.applyLeaderHint` resolves to the cache's verdict (the two hint options are unused)
 *   and `cluster.forgetLeaderEpoch` forgets the epoch there. By default the epoch stays
 *   `leaderEpoch`, routing follows `leaders`, no partition routing change reaches the group, and
 *   the forget only records the call
 * @param {boolean} [options.validationBrokers] when true, `cluster.findBroker` answers each node
 *   id with its own fake broker and connection (node `NODE_ID` keeps the shared one), so each
 *   leader has its own request queue; every broker answers with the same scripts
 * @param {string} [options.endOffset] the `EndOffset` the OffsetForLeaderEpoch reply carries
 *   with `LeaderEpoch` 4; `'-1'` is the broker's undefined answer, which carries `LeaderEpoch` -1
 * @param {number} [options.validationErrorCode] the per-partition code that reply carries; a
 *   non-zero code comes with `LeaderEpoch` -1 and `EndOffset` -1, as from a broker
 * @param {boolean} [options.validationThrows] make the OffsetForLeaderEpoch answer throw
 * @param {boolean} [options.advertiseOffsetForLeaderEpoch] whether ApiVersions lists api key 23
 * @param {number[]} [options.withholdValidationFor] partitions the reply leaves out entirely
 * @param {number} [options.preferredReadReplica] seeds every partition's preferred read replica
 * @param {Record<number, number>} [options.partitionLeaders] the leader of each partition; every
 *   partition is led by `NODE_ID` unless this says otherwise
 * @param {'classic'|'consumer'} [options.groupProtocol] the group's membership protocol
 * @param {boolean} [options.fetchSessions] whether the group uses incremental Fetch sessions
 * @param {number} [options.requestTimeout] the client request timeout a session close is capped by
 * @param {string} [options.rackId] the group's rack
 * @returns {Promise<{ teardown: () => Promise<void>, group: ConsumerGroup, cluster: object,
 *   broker: Broker, logger: object,
 *   metadataCache: MetadataCache, leaders: Map<number, number>,
 *   installRouting: (leaders: Record<number, number>) => void,
 *   offsetManager: OffsetManager, offsetCommit: jest.SpyInstance, joinAndSync: jest.SpyInstance,
 *   requestRejoin: jest.Mock,
 *   setResponse: (body: Buffer|(() => Buffer)) => void,
 *   setValidationResponse: (fn: (partitions: number[], request: { nodeId: number,
 *     partitions: { partition: number, currentLeaderEpoch: number, leaderEpoch: number }[] })
 *     => Buffer|PromiseLike<Buffer>) => void,
 *   holdBrokerLookup: (nodeId: number) => () => void,
 *   findBrokerCalls: {nodeId: number}[], sync: () => Promise<void>,
 *   requests: () => object[], validationRequests: () => object[],
 *   validationRequestsTo: (nodeId: number) => object[], writesTo: (nodeId: number) => Buffer[],
 *   fetchRequestPartitions: () => number[], fetchRequestOffsets: () => string[],
 *   lastFetchRequest: () => object[],
 *   lastFetchApiVersion: () => number, lastFetchHeaderFields: () => object,
 *   fetchFrames: () => object[], writes: () => Buffer[] }>}
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
  liveRouting = false,
  validationBrokers = false,
  endOffset = '20',
  validationErrorCode = 0,
  validationThrows = false,
  advertiseOffsetForLeaderEpoch = true,
  withholdValidationFor = [],
  preferredReadReplica,
  partitionLeaders,
  groupProtocol = 'classic',
  fetchSessions = false,
  requestTimeout,
  rackId = '',
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
        .map((partition) =>
          validationErrorCode !== 0
            ? { partition, errorCode: validationErrorCode }
            : { partition, leaderEpoch: BigInt(endOffset) === -1n ? -1 : 4, endOffset }
        )
    )
  }
  const setValidationResponse = (fn) => {
    validationAnswer = fn
  }

  /**
   * One fake broker answering for `nodeId` with the shared scripts.
   * @param {number} nodeId
   * @returns {{ broker: Broker, state: { writes: Buffer[] } }}
   */
  const createNodeBroker = (nodeId) => {
    return createFakeBroker((header, frame) => {
      if (header.apiKey === apiKeys.ApiVersions) {
        return apiVersionsBody(fetchMaxVersion, advertiseOffsetForLeaderEpoch)
      }

      if (header.apiKey === apiKeys.OffsetForLeaderEpoch) {
        const rows = decodeOffsetForLeaderEpochRequests([frame])[0].topics[0].partitions
        return validationAnswer(
          rows.map(({ partition }) => partition),
          { nodeId, partitions: rows }
        )
      }

      return typeof nextResponse === 'function' ? nextResponse() : nextResponse
    })
  }

  const { broker, state, close } = createNodeBroker(NODE_ID)
  await broker.connect()

  /** @type {Map<number, { broker: Broker, state: { writes: Buffer[] }, close: () => void,
   *   ready: Promise<void> }>} */
  const nodeBrokers = new Map([[NODE_ID, { broker, state, close, ready: Promise.resolve() }]])
  /** @type {Map<number, Set<{ released: Promise<void>, release: () => void }>>} */
  const heldLookups = new Map()
  let tornDown = false
  /**
   * Holds every `cluster.findBroker` for `nodeId` until the returned function is called. Holds on
   * one node stack: a lookup waits until every one of them is released.
   * @param {number} nodeId
   * @returns {() => void} the release
   */
  const holdBrokerLookup = (nodeId) => {
    let resolve
    const hold = { released: new Promise((done) => (resolve = done)) }
    hold.release = () => {
      const holds = heldLookups.get(nodeId)
      holds?.delete(hold)
      if (holds?.size === 0) heldLookups.delete(nodeId)
      resolve()
    }
    if (!heldLookups.has(nodeId)) heldLookups.set(nodeId, new Set())
    heldLookups.get(nodeId).add(hold)
    return hold.release
  }
  /**
   * The broker `cluster.findBroker` answers for `nodeId`: the shared one, or with
   * `validationBrokers` that node's own, created and connected on first use.
   * @param {number} nodeId
   * @returns {Promise<Broker>}
   */
  const brokerFor = async (nodeId) => {
    if (!validationBrokers) return broker
    let node = nodeBrokers.get(nodeId)
    if (!node) {
      const created = createNodeBroker(nodeId)
      node = { ...created, ready: created.broker.connect() }
      nodeBrokers.set(nodeId, node)
    }
    await node.ready
    return node.broker
  }
  const writesTo = (nodeId) => nodeBrokers.get(nodeId)?.state.writes ?? []

  const leaders = new Map(
    partitions.map((partition) => [
      partition,
      partitionLeaders ? partitionLeaders[partition] : NODE_ID,
    ])
  )
  const findBrokerCalls = []
  // The production cache publishes every routing change to the group, so a spec moves a leader by
  // installing metadata or applying a hint through it, as `Cluster` does. `leaders` is what the
  // next `installRouting` publishes; by default it is also the routing the stand-in cluster
  // answers with, and with `liveRouting` the cache's rows are, as in `Cluster`.
  const metadataCache = new MetadataCache({ logger: newLogger() })
  /**
   * @param {number} partition
   * @returns {number} the leader the stand-in cluster routes `partition` to
   */
  const leaderOf = liveRouting
    ? (partition) => metadataCache.leader(TOPIC, partition)
    : (partition) => leaders.get(partition)
  let metadataEpoch = leaderEpoch
  const routingMetadata = () => ({
    brokers: knownNodes.map((nodeId) => ({ nodeId, host: `kafka${nodeId}`, port: 9092 })),
    topicMetadata: [
      {
        topic: TOPIC,
        topicId: TOPIC_ID,
        partitionMetadata: partitions.map((partitionId) => ({
          partitionId,
          leader: leaders.get(partitionId),
          leaderEpoch: metadataEpoch,
          offlineReplicas: [],
        })),
      },
    ],
  })
  metadataCache.update(routingMetadata())
  const installRouting = (moved = {}) => {
    for (const [partition, leader] of Object.entries(moved)) leaders.set(Number(partition), leader)
    metadataEpoch += 1
    metadataCache.update(routingMetadata())
  }
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
  // With live routing both subscriptions are the `Cluster` methods over this cache, so every change
  // the cache accepts reaches the group as in production. By default the group sees one fixed
  // epoch while leaders move, and is told of no partition's routing change.
  const cluster = {
    metadataCache,
    onRoutingChange: Cluster.prototype.onRoutingChange,
    onPartitionRouted: liveRouting ? Cluster.prototype.onPartitionRouted : () => () => {},
    refreshMetadataIfNecessary: async () => {},
    refreshMetadata: jest.fn(async () => {}),
    findBroker: async (args) => {
      findBrokerCalls.push(args)
      if (tornDown) throw new Error('fetch harness torn down')
      // The default path resolves in the same turns it always did.
      if (!validationBrokers && heldLookups.size === 0) return broker
      const nodeId = Number(args.nodeId)
      while (heldLookups.has(nodeId)) {
        await heldLookups.get(nodeId).values().next().value.released
      }
      if (tornDown) throw new Error('fetch harness torn down')
      const found = await brokerFor(nodeId)
      // The node may have connected while teardown was closing it.
      if (tornDown) throw new Error('fetch harness torn down')
      return found
    },
    findGroupCoordinator: async () => coordinator,
    findTopicPartitionMetadata: () =>
      partitions.map((partitionId) => ({ partitionId, leader: leaderOf(partitionId) })),
    findLeaderForPartitions: (topic, partitionIds) =>
      partitionIds.reduce((result, id) => {
        const leader = leaderOf(Number(id))
        return { ...result, [leader]: [...(result[leader] || []), Number(id)] }
      }, {}),
    committedOffsets: () => ({}),
    defaultOffset: () => '-2',
    topicId: () => TOPIC_ID,
    leaderEpoch: liveRouting ? Cluster.prototype.leaderEpoch : () => leaderEpoch,
    cachedLeader: (topic, partition) => metadataCache.leader(topic, partition),
    forgetLeaderEpoch: jest.fn(
      liveRouting
        ? (topic, partition) => metadataCache.forgetLeaderEpoch(topic, partition)
        : () => true
    ),
    applyLeaderHint: jest.fn(
      async ({ topic, partition, leaderId, leaderEpoch: hintEpoch, topicId }) => {
        if (liveRouting) {
          // As `Cluster#applyLeaderHint`: the cache write checks the hint's topic incarnation.
          const applied = metadataCache.applyLeaderHint(topic, partition, {
            leaderId,
            leaderEpoch: hintEpoch,
            topicId,
          })
          if (applied) leaders.set(partition, leaderId)
          return applied
        }
        if (applyLeaderHintResolves && applyLeaderHintMovesLeaderTo != null) {
          leaders.set(partition, applyLeaderHintMovesLeaderTo)
        }
        metadataCache.applyLeaderHint(topic, partition, { leaderId, leaderEpoch: hintEpoch })
        return applyLeaderHintResolves
      }
    ),
    hasNode: jest.fn((nodeId) => knownNodes.includes(Number(nodeId))),
    getNodeIds: () => knownNodes,
    removeBroker: jest.fn(),
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
    groupProtocol,
    fetchSessions,
    requestTimeout,
    rackId,
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

  /** @type {Promise<void>|null} */
  let tearingDown = null
  /**
   * Ends the fixture: every held or later broker lookup rejects; each node's fake broker stops
   * answering, with no answer left queued for a later turn, once the node has finished connecting
   * (a connected node at once); and every node's connection closes at once, failing the requests
   * still waiting for an answer. For `finally` and `afterEach`, so a failed assertion cannot leave
   * a withheld answer or a held lookup pending, and no answer runs after the test. The cleanup
   * runs once; every caller waits for that same cleanup.
   * @returns {Promise<void>}
   */
  const teardown = () =>
    (tearingDown ??= (async () => {
      tornDown = true
      for (const holds of [...heldLookups.values()]) {
        for (const hold of [...holds]) hold.release()
      }
      await Promise.all(
        [...nodeBrokers.values()].map(async (node) => {
          // A connecting node needs its ApiVersions answer to finish; closing it earlier would
          // leave its connect pending.
          await node.ready.catch(() => {})
          node.close()
          await node.broker.disconnect({ drainTimeout: 0 })
        })
      )
    })())

  return {
    teardown,
    group,
    cluster,
    broker,
    logger: group.logger,
    metadataCache,
    leaders,
    installRouting,
    offsetManager: group.offsetManager,
    offsetCommit,
    joinAndSync,
    requestRejoin,
    setResponse,
    setValidationResponse,
    holdBrokerLookup,
    findBrokerCalls,
    /** Every frame written to `nodeId`'s broker, oldest first; empty before its first lookup. */
    writesTo,
    /** Every OffsetForLeaderEpoch frame written to `nodeId`'s broker, decoded, oldest first. */
    validationRequestsTo: (nodeId) => decodeOffsetForLeaderEpochRequests(writesTo(nodeId)),
    /** Drives the real join/sync sequence, and the apply that used to live inside it. */
    sync: async () => {
      const result = await realJoinAndSync()
      return group.applyRebalanceResult(result)
    },
    /** The header of every request written to the socket, oldest first. */
    requests: () => state.writes.map(readRequestHeader),
    /** Every OffsetForLeaderEpoch frame written to the socket, decoded, oldest first. */
    validationRequests: () => decodeOffsetForLeaderEpochRequests(state.writes),
    /** Every frame written to the socket, oldest first. */
    writes: () => state.writes,
    /** Every Fetch frame written to the socket, decoded, oldest first. */
    fetchFrames: () =>
      state.writes
        .filter((write) => readRequestHeader(write).apiKey === apiKeys.Fetch)
        .map((write) => decodeFetchRequest([write])),
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
  buildRecordBatch,
  apiVersionsBody,
  decodeFetchRequest,
  decodeOffsetForLeaderEpochRequests,
  createFakeBroker,
  createFakeCoordinator,
  withheldAnswer,
  fetchResponse,
  offsetForLeaderEpochResponse,
  offsetForLeaderEpochRawResponse,
  scriptedEpochLog,
  createFetchGroup,
}
