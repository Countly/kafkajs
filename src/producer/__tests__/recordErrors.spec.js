const Broker = require('../../broker')
const Connection = require('../../network/connection')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { responseHeaderVersion } = require('../../protocol/headers')
const retry = require('../../retry')
const createSendMessages = require('../sendMessages')
const {
  newLogger,
  createFakeSocketFactory,
  respondWithFrame,
  readRequestHeader,
} = require('testHelpers')

const TOPIC = 'test-topic'
const NODE_ID = 0

/** A v3/v4-shaped ApiVersions body advertising the given Produce maxVersion. */
const apiVersionsBody = (produceMaxVersion) =>
  new Encoder()
    .writeInt16(0)
    .writeUVarIntArray([
      new Encoder()
        .writeInt16(apiKeys.Produce)
        .writeInt16(0)
        .writeInt16(produceMaxVersion)
        .writeTaggedFields(),
    ])
    .writeInt32(0)
    .writeTaggedFields().buffer

/**
 * Builds a real `Broker` wired to a real `Connection` over the fake socket from `testHelpers`.
 * `answer` receives each outgoing request's header and returns the response body to play back;
 * the response header version is derived per-request from `responseHeaderVersion`, so a Produce
 * request negotiated at v9 gets a flexible (v1) response header while v8 gets the classic one.
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
      const headerVersion = responseHeaderVersion(header.apiKey, header.apiVersion)
      respondWithFrame(state, header.correlationId, headerVersion, answer(header))
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
    send: async (protocolRequest) => {
      const promise = connection.send(protocolRequest)
      await new Promise(setImmediate)
      await pump()
      return promise
    },
    setVersions: () => {},
    destroy: async () => connection.disconnect(),
  }

  return { broker: new Broker({ connectionPool, logger: newLogger(), nodeId: NODE_ID }), state }
}

/** Minimal `Cluster` stub: every partition of `TOPIC` is led by the one fake broker. */
const createCluster = (broker, partitionMetadata) => ({
  addMultipleTargetTopics: async () => {},
  refreshMetadataIfNecessary: async () => {},
  refreshMetadata: async () => {},
  findTopicPartitionMetadata: () => partitionMetadata,
  findLeaderForPartitions: (topic, partitions) => ({ [NODE_ID]: partitions.map(Number) }),
  findBroker: async () => broker,
  targetTopics: new Set(),
  isConnected: () => true,
})

const createEosManager = () => ({
  getProducerId: () => -1,
  getProducerEpoch: () => -1,
  getSequence: () => 0,
  getTransactionalId: () => undefined,
  updateSequence: () => {},
  isTransactional: () => false,
  addPartitionsToTransaction: async () => {},
  acquireBrokerLock: async () => {},
  releaseBrokerLock: async () => {},
})

/** Routes each message to the partition explicitly set on it, bypassing any real partitioner. */
const partitionerByMessage = ({ message }) => message.partition

describe('Producer > sendMessages > recordErrors (KIP-467, real codec over a fake broker)', () => {
  test('a v9 (flexible) failure resolves recordErrors[].record through the per-partition grouping', async () => {
    const messages = [
      { key: 'a', value: '1', partition: 0 },
      { key: 'b', value: '2', partition: 0 },
      { key: 'c', value: '3', partition: 0 },
    ]

    const answer = (header) =>
      header.apiKey === apiKeys.ApiVersions
        ? apiVersionsBody(9)
        : Buffer.from(
            require('../../protocol/requests/produce/fixtures/v9_response_record_errors.json')
          )

    const { broker, state } = createFakeBroker(answer)
    await broker.connect()

    const cluster = createCluster(broker, [{ partitionId: 0, leader: NODE_ID }])
    const sendMessages = createSendMessages({
      logger: newLogger(),
      cluster,
      partitioner: partitionerByMessage,
      eosManager: createEosManager(),
      retrier: retry({ retries: 0 }),
    })

    const error = await sendMessages({
      acks: -1,
      timeout: 30000,
      topicMessages: [{ topic: TOPIC, messages }],
    }).catch((e) => e)

    // Pins the version this case is actually exercising, rather than leaving it implicit in the
    // fixture choice: this is the flexible (v9) request/response path, not just "some" version.
    expect(readRequestHeader(state.writes[1]).apiVersion).toEqual(9)

    expect(error).toBeInstanceOf(Error)
    expect(error.recordErrors).toEqual([
      { batchIndex: 1, record: messages[1], errorMessage: 'record too large' },
    ])
    expect(error.recordErrors[0].record).toBe(messages[1])
    expect(error.recordErrors[0].record).not.toBe(messages[0])
  })

  test("a v8 (classic) failure on a non-first partition resolves through that partition's own grouping", async () => {
    // Six messages split three-to-a-partition; the fixture's failing partition is 1 with
    // batchIndex 0, so the correct record is the *fourth* message overall. A mapping that indexed
    // the caller's flat message array (instead of the per-partition grouping) would return the
    // first message here instead, which is the only reason this case exists.
    const messages = Array.from({ length: 6 }, (_, i) => ({
      key: `k${i}`,
      value: `v${i}`,
      partition: i < 3 ? 0 : 1,
    }))

    const answer = (header) =>
      header.apiKey === apiKeys.ApiVersions
        ? apiVersionsBody(8)
        : Buffer.from(
            require('../../protocol/requests/produce/fixtures/v8_response_record_errors_partition1.json')
          )

    const { broker, state } = createFakeBroker(answer)
    await broker.connect()

    const cluster = createCluster(broker, [
      { partitionId: 0, leader: NODE_ID },
      { partitionId: 1, leader: NODE_ID },
    ])
    const sendMessages = createSendMessages({
      logger: newLogger(),
      cluster,
      partitioner: partitionerByMessage,
      eosManager: createEosManager(),
      retrier: retry({ retries: 0 }),
    })

    const error = await sendMessages({
      acks: -1,
      timeout: 30000,
      topicMessages: [{ topic: TOPIC, messages }],
    }).catch((e) => e)

    // Pins the version this case is actually exercising: the classic (v8) request/response path.
    expect(readRequestHeader(state.writes[1]).apiVersion).toEqual(8)

    expect(error.partition).toEqual(1)
    expect(error.recordErrors).toEqual([
      { batchIndex: 0, record: messages[3], errorMessage: 'record too large' },
    ])
    expect(error.recordErrors[0].record).toBe(messages[3])
  })
})
