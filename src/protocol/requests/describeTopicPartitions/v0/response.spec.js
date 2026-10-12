const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')
const { KafkaJSPartialMessageError } = require('../../../../errors')

const CASES = [
  'elrNull',
  'elrEmpty',
  'elrPopulated',
  'nextCursor',
  'noNextCursor',
  'unauthorizedZeroId',
]
const vector = (caseId) =>
  require(`../fixtures/java/describeTopicPartitionsResponse-v0-${caseId}.json`)

/** One partition row; the fields default to partition 0 led by broker 1 with one replica. */
const partitionBytes = ({
  errorCode = 0,
  index = 0,
  leader = 1,
  epoch = 5,
  replicas = [1],
  isr = [1],
  elr = null,
  lastKnownElr = null,
  offline = [],
  tags,
} = {}) =>
  new Encoder()
    .writeInt16(errorCode)
    .writeInt32(index)
    .writeInt32(leader)
    .writeInt32(epoch)
    .writeUVarIntArray(replicas, 'int32')
    .writeUVarIntArray(isr, 'int32')
    .writeUVarIntArray(elr, 'int32')
    .writeUVarIntArray(lastKnownElr, 'int32')
    .writeUVarIntArray(offline, 'int32')
    .writeTaggedFields(tags)

/** One topic row; `partitions` is an array of encoders or `null` for the compact null. */
const topicBytes = ({ name = 'topic', partitions = [], operations = -2147483648, tags } = {}) =>
  new Encoder()
    .writeInt16(0)
    .writeUVarIntString(name)
    .writeUuid(null)
    .writeBoolean(false)
    .writeUVarIntArray(partitions)
    .writeInt32(operations)
    .writeTaggedFields(tags)

/** A cursor as the nullable struct: the marker byte, then the body when present. */
const cursorBytes = (cursor) =>
  cursor == null
    ? new Encoder().writeInt8(-1)
    : new Encoder()
        .writeInt8(1)
        .writeUVarIntString(cursor.topicName)
        .writeInt32(cursor.partitionIndex)
        .writeTaggedFields()

/** A whole response body; `topics` is an array of encoders or `null` for the compact null. */
const bodyOf = ({ topics, nextCursor = null, tags }) =>
  new Encoder()
    .writeInt32(0)
    .writeUVarIntArray(topics)
    .writeEncoder(cursorBytes(nextCursor))
    .writeTaggedFields(tags).buffer

describe('Protocol > Requests > DescribeTopicPartitions > v0 > response', () => {
  test.each(CASES)('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    await expect(decode(Buffer.from(hex, 'hex'))).resolves.toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      topics: fields.topics,
      nextCursor: fields.nextCursor,
    })
  })

  test('tells a null list of eligible leader replicas from an empty one and a full one', async () => {
    const elrOf = async (caseId) => {
      const { topics } = await decode(Buffer.from(vector(caseId).hex, 'hex'))
      const [{ eligibleLeaderReplicas, lastKnownElr }] = topics[0].partitions
      return { eligibleLeaderReplicas, lastKnownElr }
    }

    expect(await elrOf('elrNull')).toEqual({ eligibleLeaderReplicas: null, lastKnownElr: null })
    expect(await elrOf('elrEmpty')).toEqual({ eligibleLeaderReplicas: [], lastKnownElr: [] })
    expect(await elrOf('elrPopulated')).toEqual({ eligibleLeaderReplicas: [1], lastKnownElr: [2] })
  })

  test('reads the zero topic id of an unauthorized topic as null', async () => {
    const { topics } = await decode(Buffer.from(vector('unauthorizedZeroId').hex, 'hex'))

    expect(topics).toEqual([
      expect.objectContaining({ errorCode: 29, topicId: null, partitions: [] }),
    ])
  })

  test('keeps an empty topic name, an empty cursor name and a null name apart', async () => {
    const body = bodyOf({
      topics: [topicBytes({ name: '' }), topicBytes({ name: null })],
      nextCursor: { topicName: '', partitionIndex: 3 },
    })

    const { topics, nextCursor } = await decode(body)

    expect(topics.map(({ name }) => name)).toEqual(['', null])
    expect(nextCursor).toEqual({ topicName: '', partitionIndex: 3 })
  })

  test('reads a compact null count of a list that is not nullable as an empty list', async () => {
    const nullCount = new Encoder().writeUVarInt(0)
    const partition = new Encoder()
      .writeInt16(0)
      .writeInt32(0)
      .writeInt32(1)
      .writeInt32(5)
      .writeEncoder(nullCount)
      .writeEncoder(nullCount)
      .writeUVarIntArray(null)
      .writeUVarIntArray(null)
      .writeEncoder(nullCount)
      .writeTaggedFields()
    const withNullPartitions = topicBytes({ partitions: null })
    const { topics } = await decode(
      new Encoder()
        .writeInt32(0)
        .writeUVarIntArray([withNullPartitions, topicBytes({ partitions: [partition] })])
        .writeEncoder(cursorBytes(null))
        .writeTaggedFields().buffer
    )
    const [first, second] = topics

    expect(first.partitions).toEqual([])
    expect(second.partitions[0]).toMatchObject({
      replicaNodes: [],
      isrNodes: [],
      eligibleLeaderReplicas: null,
      lastKnownElr: null,
      offlineReplicas: [],
    })
    await expect(decode(bodyOf({ topics: null }))).resolves.toMatchObject({ topics: [] })
  })

  test('decodes the partition index, error code and authorized operations as sent', async () => {
    const body = bodyOf({
      topics: [
        topicBytes({
          name: 'orders',
          operations: 1032,
          partitions: [
            partitionBytes({ index: 3, leader: 2, epoch: 9, replicas: [2, 4], isr: [2, 4] }),
            partitionBytes({
              errorCode: 9,
              index: 7,
              leader: -1,
              epoch: 11,
              replicas: [4],
              isr: [],
              offline: [4],
            }),
          ],
        }),
      ],
    })

    const { topics } = await decode(body)

    expect(topics[0]).toMatchObject({ name: 'orders', topicAuthorizedOperations: 1032 })
    expect(topics[0].partitions).toEqual([
      {
        errorCode: 0,
        partitionIndex: 3,
        leaderId: 2,
        leaderEpoch: 9,
        replicaNodes: [2, 4],
        isrNodes: [2, 4],
        eligibleLeaderReplicas: null,
        lastKnownElr: null,
        offlineReplicas: [],
      },
      {
        errorCode: 9,
        partitionIndex: 7,
        leaderId: -1,
        leaderEpoch: 11,
        replicaNodes: [4],
        isrNodes: [],
        eligibleLeaderReplicas: null,
        lastKnownElr: null,
        offlineReplicas: [4],
      },
    ])
  })

  test('skips an unknown tagged field at every level', async () => {
    const tags = [{ tag: 7, value: Buffer.from([1, 2, 3]) }]
    const body = bodyOf({
      topics: [topicBytes({ partitions: [partitionBytes({ tags })], tags })],
      nextCursor: { topicName: 'topic', partitionIndex: 1 },
      tags,
    })

    const { topics, nextCursor } = await decode(body)

    expect(topics[0].partitions).toHaveLength(1)
    expect(nextCursor).toEqual({ topicName: 'topic', partitionIndex: 1 })
  })

  test('refuses a cursor marker that is neither -1 nor 1, rather than read on from a wrong offset', async () => {
    const body = new Encoder()
      .writeInt32(0)
      .writeUVarIntArray([])
      .writeInt8(2)
      .writeTaggedFields().buffer

    await expect(decode(body)).rejects.toThrow(KafkaJSPartialMessageError)
    await expect(decode(body)).rejects.toThrow(/NextCursor presence marker 2/)
  })

  test('parse returns the data, also when a topic carries an error code', async () => {
    const data = await decode(Buffer.from(vector('unauthorizedZeroId').hex, 'hex'))

    await expect(parse(data)).resolves.toBe(data)
  })

  describe.each(CASES)('a response cut short (%s)', (caseId) => {
    const { hex } = vector(caseId)
    const whole = Buffer.from(hex, 'hex')

    test('is refused at every length below its own, with a typed error and never a native one', async () => {
      for (let length = 0; length < whole.length; length += 1) {
        await expect(decode(whole.subarray(0, length))).rejects.toThrow(KafkaJSPartialMessageError)
      }
    })
  })
})
