const Encoder = require('../../../encoder')
const responseV2 = require('./response')
const { protocol } = require('../index')

const request = require('../fixtures/java/deleteRecordsRequest-v2-basic.json')
const vector = (caseId) => require(`../fixtures/java/deleteRecordsResponse-v2-${caseId}.json`)

/** The decoded shape of a vector's partitions: int64 values are bigints. */
const decodedTopics = (topics) =>
  topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map((partition) => ({
      ...partition,
      lowWatermark: BigInt(partition.lowWatermark),
    })),
  }))

describe('Protocol > Requests > DeleteRecords > v2 response', () => {
  const { decode, parse } = responseV2({ topics: request.fields.topics })

  test.each(['ok', 'error'])('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      topics: decodedTopics(fields.topics),
    })
  })

  test('parse resolves a clean response', async () => {
    const data = await decode(Buffer.from(vector('ok').hex, 'hex'))

    await expect(parse(data)).resolves.toBe(data)
  })

  test('parse throws the failed partition with the offset the request carried', async () => {
    const data = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(data)).rejects.toMatchObject({
      name: 'KafkaJSDeleteTopicRecordsError',
      partitions: [{ partition: 2, offset: '-1', error: expect.objectContaining({ code: 1 }) }],
    })
  })

  test('is built with the request topics through the version map', async () => {
    const { response } = protocol({ version: 2 })(request.fields)
    const data = await response.decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(response.parse(data)).rejects.toMatchObject({
      name: 'KafkaJSDeleteTopicRecordsError',
    })
  })

  test('sorts the topics by name as v0 does', async () => {
    const body = new Encoder()
      .writeInt32(0)
      .writeUVarIntArray(
        ['b', 'a'].map((name) =>
          new Encoder().writeUVarIntString(name).writeUVarIntArray([]).writeTaggedFields()
        )
      )
      .writeTaggedFields().buffer

    expect((await decode(body)).topics.map(({ topic }) => topic)).toEqual(['a', 'b'])
  })

  test('keeps an empty topic name as an empty string, never as null', async () => {
    const body = new Encoder()
      .writeInt32(25)
      .writeUVarIntArray([
        new Encoder().writeUVarIntString('').writeUVarIntArray([]).writeTaggedFields(),
      ])
      .writeTaggedFields().buffer

    expect(await decode(body)).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      topics: [{ topic: '', partitions: [] }],
    })
  })

  test('reads a compact null count as no topics, never as a native error', async () => {
    const body = new Encoder().writeInt32(25).writeUVarIntArray(null).writeTaggedFields().buffer

    expect(await decode(body)).toEqual({ throttleTime: 0, clientSideThrottleTime: 25, topics: [] })
  })
})
