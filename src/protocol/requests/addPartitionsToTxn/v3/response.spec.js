const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/addPartitionsToTxnResponse-v3-${caseId}.json`)

/**
 * Writes a v3 response body from a vector's values: the other direction of the byte check.
 * @param {{ throttleTime: number, errors: { topic: string, partitionErrors: { partition: number, errorCode: number }[] }[] }} fields
 * @param {{ topicTag?: Buffer, partitionTag?: Buffer }} [tags] a tagged field to add to every topic
 *   result and to every partition result
 * @returns {Buffer}
 */
const encodeResponse = ({ throttleTime, errors }, { topicTag, partitionTag } = {}) =>
  new Encoder()
    .writeInt32(throttleTime)
    .writeUVarIntArray(
      errors.map(({ topic, partitionErrors }) =>
        new Encoder()
          .writeUVarIntString(topic)
          .writeUVarIntArray(
            partitionErrors.map(({ partition, errorCode }) =>
              new Encoder()
                .writeInt32(partition)
                .writeInt16(errorCode)
                .writeTaggedFields(partitionTag ? [{ tag: 0, value: partitionTag }] : [])
            )
          )
          .writeTaggedFields(topicTag ? [{ tag: 0, value: topicTag }] : [])
      )
    )
    .writeTaggedFields().buffer

describe('Protocol > Requests > AddPartitionsToTxn > v3 response', () => {
  test.each(['ok', 'error'])(
    'decodes the Apache 4.3.1 %s vector into the v1 shape',
    async (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        errors: fields.errors,
      })
    }
  )

  test.each(['ok', 'error'])('the %s values encode to the vector bytes', (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(encodeResponse(fields).toString('hex')).toEqual(hex)
  })

  test('skips a tagged field a newer broker adds to a partition result', async () => {
    const { fields } = vector('ok')

    expect(
      await decode(encodeResponse(fields, { partitionTag: Buffer.from([0xab, 0xcd]) }))
    ).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      errors: fields.errors,
    })
  })

  test('decodes every topic of a multi-topic response, past a tagged field on a topic result', async () => {
    const fields = {
      throttleTime: 25,
      errors: [
        { topic: 'a', partitionErrors: [{ partition: 0, errorCode: 0 }] },
        {
          topic: 'bb',
          partitionErrors: [
            { partition: 1, errorCode: 0 },
            { partition: 2, errorCode: 51 },
          ],
        },
        { topic: 'ccc', partitionErrors: [] },
      ],
    }

    expect(await decode(encodeResponse(fields, { topicTag: Buffer.from([0xab, 0xcd]) }))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      errors: fields.errors,
    })
    expect(await decode(encodeResponse(fields))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      errors: fields.errors,
    })
  })

  test('parse resolves when no partition failed', async () => {
    const data = await decode(Buffer.from(vector('ok').hex, 'hex'))

    await expect(parse(data)).resolves.toEqual(data)
  })

  test('parse throws the failed partition error', async () => {
    const data = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(data)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'CONCURRENT_TRANSACTIONS',
      code: 51,
    })
  })
})
