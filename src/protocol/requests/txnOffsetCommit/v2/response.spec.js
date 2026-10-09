const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/txnOffsetCommitResponse-v2-${caseId}.json`)

/**
 * Writes a v2 response body from a vector's values: the other direction of the byte check.
 * @param {{ throttleTime: number, topics: { topic: string, partitions: { partition: number, errorCode: number }[] }[] }} fields
 * @returns {Buffer}
 */
const encodeResponse = ({ throttleTime, topics }) =>
  new Encoder()
    .writeInt32(throttleTime)
    .writeArray(
      topics.map(({ topic, partitions }) =>
        new Encoder()
          .writeString(topic)
          .writeArray(
            partitions.map(({ partition, errorCode }) =>
              new Encoder().writeInt32(partition).writeInt16(errorCode)
            )
          )
      )
    ).buffer

describe('Protocol > Requests > TxnOffsetCommit > v2 response', () => {
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
