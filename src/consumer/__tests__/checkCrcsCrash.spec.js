const Broker = require('../../broker')
const Connection = require('../../network/connection')
const Encoder = require('../../protocol/encoder')
const ConsumerGroup = require('../consumerGroup')
const OffsetManager = require('../offsetManager')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const apiKeys = require('../../protocol/requests/apiKeys')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const {
  newLogger,
  createFakeSocketFactory,
  respondWithFrame,
  readRequestHeader,
} = require('testHelpers')

const TOPIC = 'test-topic'
const NODE_ID = 0

/** A v3/v4-shaped ApiVersions body advertising a high enough Fetch maxVersion to negotiate v11. */
const apiVersionsBody = () =>
  new Encoder()
    .writeInt16(0)
    .writeUVarIntArray([
      new Encoder().writeInt16(apiKeys.Fetch).writeInt16(0).writeInt16(11).writeTaggedFields(),
    ])
    .writeInt32(0)
    .writeTaggedFields().buffer

/**
 * Builds a real record batch (production encoder, one record) and flips a byte inside its
 * CRC32C-covered region without touching record content -- same technique as
 * `src/protocol/recordBatch/v0/crcValidation.spec.js`, kept here as a small local helper.
 * @param {boolean} corrupted
 * @returns {Promise<Buffer>}
 */
const buildBatch = async (corrupted) => {
  const batch = Buffer.from(
    (
      await RecordBatch({
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: [RecordEncoder({ key: Buffer.from('key'), value: Buffer.from('value') })],
      })
    ).buffer
  )

  if (corrupted) {
    // FirstOffset(8) + Length(4) + PartitionLeaderEpoch(4) + Magic(1) + CRC(4) + Attributes(2) +
    // LastOffsetDelta(4) + FirstTimestamp(8) + MaxTimestamp(8) + ProducerId(8): the next byte
    // (ProducerEpoch) is inside the covered region but is plain carried-through metadata.
    const PRODUCER_EPOCH_OFFSET = 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8
    batch[PRODUCER_EPOCH_OFFSET] ^= 0x01
  }

  return batch
}

/** A Fetch v11 response body carrying a single topic-partition with the given record batch. */
const fetchResponseBody = (batch) =>
  new Encoder()
    .writeInt32(0) // clientSideThrottleTime
    .writeInt16(0) // errorCode
    .writeInt32(0) // sessionId
    .writeArray(
      [
        new Encoder().writeString(TOPIC).writeArray(
          [
            new Encoder()
              .writeInt32(0) // partition
              .writeInt16(0) // errorCode
              .writeInt64(10) // highWatermark
              .writeInt64(10) // lastStableOffset
              .writeInt64(0) // logStartOffset
              .writeArray([], 'object') // abortedTransactions
              .writeInt32(-1) // preferredReadReplica
              .writeBytes(batch),
          ],
          'object'
        ),
      ],
      'object'
    ).buffer

/**
 * Builds a real `Broker` wired to a real `Connection` over the fake socket from `testHelpers`.
 * `answer` receives each outgoing request's header and returns the response body to
 * play back; every response here is framed with header v0, matching both ApiVersions (always v0,
 * KIP-511) and Fetch (not made flexible in this phase).
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
      respondWithFrame(state, header.correlationId, 0, answer(header))
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
 * Builds a `ConsumerGroup` whose join/sync dance is skipped: `subscriptionState` and
 * `offsetManager` are primed directly (mirroring what a real join would have produced), and
 * `cluster` is a minimal stub that routes `findBroker` to the given real, fake-socket-backed
 * broker. This still exercises every hop from `RecordBatchDecoder` through the Fetch codec,
 * `Broker#fetch` and `recoverFromFetch`'s tail `throw` -- only the socket and the group
 * coordination protocol are stubbed.
 * @param {import('../../broker')} broker
 * @param {{ checkCrcs: boolean }} options
 */
const createConsumerGroup = (broker, { checkCrcs }) => {
  const cluster = {
    refreshMetadataIfNecessary: async () => {},
    refreshMetadata: async () => {},
    findBroker: async () => broker,
    findTopicPartitionMetadata: () => [{ partitionId: 0, leader: NODE_ID }],
    committedOffsets: () => ({}),
    topicId: () => undefined,
    leaderEpoch: () => -1,
  }

  const consumerGroup = new ConsumerGroup({
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
    checkCrcs,
    metadataMaxAge: 300000,
  })

  consumerGroup.subscriptionState.assign([{ topic: TOPIC, partitions: [0] }])
  consumerGroup.assignmentEpoch += 1
  for (const partition of [0]) {
    const acquiredAt = ++consumerGroup.acquireClock
    consumerGroup.acquired.set(`${TOPIC}|${partition}`, { acquiredAt, changedAt: acquiredAt })
  }
  consumerGroup.offsetManager = new OffsetManager({
    cluster,
    coordinator: broker,
    memberAssignment: { [TOPIC]: [0] },
    autoCommit: false,
    topicConfigurations: { [TOPIC]: {} },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'test-group',
    generationId: 0,
    memberId: 'member-1',
  })
  // A real join would have resolved this from OffsetFetch; priming it directly is what lets this
  // scenario skip the join dance while keeping the Fetch codec path completely real.
  consumerGroup.offsetManager.committedOffsets()[TOPIC][0] = '0'

  return consumerGroup
}

const answerWith = (batch) => (header) =>
  header.apiKey === apiKeys.ApiVersions ? apiVersionsBody() : fetchResponseBody(batch)

describe('Consumer > checkCrcs (consumer group + real Fetch codec over a fake broker)', () => {
  test('a corrupt batch halts its partition, delivers nothing from it and does not advance the offset', async () => {
    const corruptBatch = await buildBatch(true)
    const { broker, state } = createFakeBroker(answerWith(corruptBatch))
    await broker.connect()
    const consumerGroup = createConsumerGroup(broker, { checkCrcs: true })
    const halted = []
    consumerGroup.instrumentationEmitter.addListener('consumer.partition_halted', (event) =>
      halted.push(event)
    )

    // The fetch resolves: a checksum mismatch no longer crashes the consumer.
    const batches = await consumerGroup.fetch(NODE_ID)

    // 1. No message of the corrupt batch reaches a handler.
    expect(batches.flatMap(({ messages }) => messages)).toEqual([])

    // 2. The partition is halted at the batch, once, with the reason and no offset to skip to.
    expect(consumerGroup.haltedPartitions.get(`${TOPIC}|0`)).toMatchObject({
      reason: 'checksum',
      offset: '0',
    })
    expect(halted).toHaveLength(1)
    expect(halted[0].payload.error).toMatchObject({
      name: 'KafkaJSCorruptRecordError',
      reason: 'checksum',
      retriable: false,
    })
    expect(halted[0].payload.nextOffset).toBeUndefined()

    // 3. No OffsetCommit was ever written: the offset is not advanced past the corrupt batch.
    const offsetCommits = state.writes.filter(
      (frame) => readRequestHeader(frame).apiKey === apiKeys.OffsetCommit
    )
    expect(offsetCommits).toHaveLength(0)
  })

  test('checkCrcs: false delivers the same corrupt batch instead of crashing', async () => {
    // The same corrupt batch as the test above: with checkCrcs off, the corruption (a flipped
    // byte in plain carried-through metadata, not record content) never fails a check, so the
    // message reaches the caller instead of a KafkaJSPartialMessageError being thrown.
    const corruptBatch = await buildBatch(true)
    const { broker } = createFakeBroker(answerWith(corruptBatch))
    await broker.connect()
    const consumerGroup = createConsumerGroup(broker, { checkCrcs: false })

    const batches = await consumerGroup.fetch(NODE_ID)

    expect(batches).toHaveLength(1)
    expect(batches[0].messages).toHaveLength(1)
  })
})
