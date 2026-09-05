const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'

const int32 = (v) => new Encoder().writeInt32(v)

const broker = (tags = []) =>
  new Encoder()
    .writeInt32(0)
    .writeUVarIntString('127.0.0.1')
    .writeInt32(9092)
    .writeUVarIntString(null)
    .writeTaggedFields(tags)

const partition = ({ replicas = [1], isr = [1], offlineReplicas = [], tags = [] } = {}) =>
  new Encoder()
    .writeInt16(0)
    .writeInt32(0)
    .writeInt32(1)
    .writeInt32(5)
    .writeUVarIntArray(replicas === null ? null : replicas.map(int32))
    .writeUVarIntArray(isr === null ? null : isr.map(int32))
    .writeUVarIntArray(offlineReplicas === null ? null : offlineReplicas.map(int32))
    .writeTaggedFields(tags)

const topic = ({ name = 'test-topic', partitions = [partition()], tags = [] } = {}) =>
  new Encoder()
    .writeInt16(0)
    .writeUVarIntString(name)
    .writeUuid(TOPIC_ID)
    .writeBoolean(false)
    .writeUVarIntArray(partitions === null ? null : partitions)
    .writeInt32(-2147483648)
    .writeTaggedFields(tags)

const response = ({ brokers = [broker()], topics = [topic()], tags = [] } = {}) =>
  new Encoder()
    .writeInt32(0)
    .writeUVarIntArray(brokers === null ? null : brokers)
    .writeUVarIntString('test-cluster-id')
    .writeInt32(0)
    .writeUVarIntArray(topics === null ? null : topics)
    .writeTaggedFields(tags).buffer

const expectedTopic = (overrides = {}) => ({
  topicErrorCode: 0,
  topic: 'test-topic',
  topicId: TOPIC_ID,
  isInternal: false,
  topicAuthorizedOperations: -2147483648,
  partitionMetadata: [
    {
      partitionErrorCode: 0,
      partitionId: 0,
      leader: 1,
      leaderEpoch: 5,
      replicas: [1],
      isr: [1],
      offlineReplicas: [],
    },
  ],
  ...overrides,
})

describe('Protocol > Requests > Metadata > v12', () => {
  test('decodes the vector produced by the Apache kafka-clients 4.3.1 codec', async () => {
    const fromJava = await decode(Buffer.from(require('../fixtures/v12_response_java.json')))
    const fromHand = await decode(Buffer.from(require('../fixtures/v12_response.json')))

    expect(fromJava).toEqual(fromHand)
  })

  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v12_response.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      brokers: [{ nodeId: 0, host: '127.0.0.1', port: 9092, rack: null }],
      clusterId: 'test-cluster-id',
      controllerId: 0,
      topicMetadata: [expectedTopic()],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('non-empty tag sections at every struct end do not corrupt the decoded values', async () => {
    const buffer = response({
      brokers: [broker([{ tag: 5, value: Buffer.from([9, 9]) }])],
      topics: [
        topic({
          tags: [{ tag: 3, value: Buffer.from([2, 2]) }],
          partitions: [partition({ tags: [{ tag: 7, value: Buffer.from([1]) }] })],
        }),
      ],
      tags: [{ tag: 11, value: Buffer.from([4]) }],
    })

    const data = await decode(buffer)

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      brokers: [{ nodeId: 0, host: '127.0.0.1', port: 9092, rack: null }],
      clusterId: 'test-cluster-id',
      controllerId: 0,
      topicMetadata: [expectedTopic()],
    })
  })

  test('a null topic Name decodes to null (nullable from v12)', async () => {
    const buffer = response({ topics: [topic({ name: null })] })
    const data = await decode(buffer)

    expect(data.topicMetadata[0].topic).toBeNull()
  })

  test('null top-level arrays decode as empty arrays', async () => {
    const buffer = response({ brokers: null, topics: null })
    const data = await decode(buffer)

    expect(data.brokers).toEqual([])
    expect(data.topicMetadata).toEqual([])
  })

  test('null partition-list and null replica/isr/offlineReplicas arrays decode as empty arrays', async () => {
    const buffer = response({
      topics: [
        topic({ name: 'no-partitions', partitions: null }),
        topic({
          name: 'null-partition-arrays',
          partitions: [partition({ replicas: null, isr: null, offlineReplicas: null })],
        }),
      ],
    })

    const data = await decode(buffer)

    expect(data.topicMetadata[0].partitionMetadata).toEqual([])
    expect(data.topicMetadata[1].partitionMetadata[0]).toEqual({
      partitionErrorCode: 0,
      partitionId: 0,
      leader: 1,
      leaderEpoch: 5,
      replicas: [],
      isr: [],
      offlineReplicas: [],
    })
  })

  test.each([
    ['4.3.1', '../fixtures/v12_response_kafka431.json'],
    ['4.1.2', '../fixtures/v12_response_kafka412.json'],
  ])('decodes a real broker response from Kafka %s', async (_version, fixture) => {
    const data = await decode(Buffer.from(require(fixture)))

    expect(data.clusterId).toEqual('kafkajs-capture-clstr0')
    expect(data.brokers.length).toBeGreaterThan(0)
    const topic = data.topicMetadata.find((entry) => entry.topic === 'kafkajs-capture-topic')
    expect(topic.topicId).toEqual('01020304-0506-0708-090a-0b0c0d0e0f10')
    expect(topic.topicAuthorizedOperations).toEqual(-2147483648)
    expect(topic.partitionMetadata[0].leaderEpoch).toBeGreaterThanOrEqual(0)
  })
})
