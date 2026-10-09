const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/txnOffsetCommitResponse-v3-${caseId}.json`)

/**
 * Writes a v3 response body from a vector's values: the other direction of the byte check.
 * @param {{ throttleTime: number, topics: { topic: string, partitions: { partition: number, errorCode: number }[] }[] }} fields
 * @param {{ tag: number, value: Buffer }[]} [tags] tagged fields for the response's own buffer
 * @returns {Buffer}
 */
const encodeResponse = ({ throttleTime, topics }, tags = []) =>
  new Encoder()
    .writeInt32(throttleTime)
    .writeUVarIntArray(
      topics.map(({ topic, partitions }) =>
        new Encoder()
          .writeUVarIntString(topic)
          .writeUVarIntArray(
            partitions.map(({ partition, errorCode }) =>
              new Encoder().writeInt32(partition).writeInt16(errorCode).writeTaggedFields()
            )
          )
          .writeTaggedFields()
      )
    )
    .writeTaggedFields(tags).buffer

describe('Protocol > Requests > TxnOffsetCommit > v3 response', () => {
  test.each(['ok', 'error'])(
    'decodes the Apache 4.3.1 %s vector into the v1 shape',
    async (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        topics: fields.topics,
      })
    }
  )

  test.each(['ok', 'error'])('the %s values encode to the vector bytes', (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(encodeResponse(fields).toString('hex')).toEqual(hex)
  })

  test('decodes several topics and partitions, each closed by its own tag buffer', async () => {
    const topics = [
      {
        topic: 'topic-a',
        partitions: [
          { partition: 0, errorCode: 0 },
          { partition: 1, errorCode: 22 },
        ],
      },
      { topic: 'topic-b', partitions: [{ partition: 7, errorCode: 120 }] },
    ]

    expect(await decode(encodeResponse({ throttleTime: 25, topics }))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      topics,
    })
  })

  test('skips a tagged field a newer broker adds', async () => {
    const { fields } = vector('ok')
    const tagged = encodeResponse(fields, [{ tag: 3, value: Buffer.from([0x01]) }])

    expect(await decode(tagged)).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      topics: fields.topics,
    })
  })

  test('parse resolves without an error code and throws ILLEGAL_GENERATION from the error vector', async () => {
    const ok = await decode(Buffer.from(vector('ok').hex, 'hex'))
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(ok)).resolves.toEqual(ok)
    await expect(parse(failed)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'ILLEGAL_GENERATION',
      code: 22,
    })
  })
})
