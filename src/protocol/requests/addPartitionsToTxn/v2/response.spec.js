const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/addPartitionsToTxnResponse-v2-${caseId}.json`)

/**
 * Writes a v2 response body from a vector's values: the other direction of the byte check.
 * @param {{ throttleTime: number, errors: { topic: string, partitionErrors: { partition: number, errorCode: number }[] }[] }} fields
 * @returns {Buffer}
 */
const encodeResponse = ({ throttleTime, errors }) =>
  new Encoder()
    .writeInt32(throttleTime)
    .writeArray(
      errors.map(({ topic, partitionErrors }) =>
        new Encoder()
          .writeString(topic)
          .writeArray(
            partitionErrors.map(({ partition, errorCode }) =>
              new Encoder().writeInt32(partition).writeInt16(errorCode)
            )
          )
      )
    ).buffer

describe('Protocol > Requests > AddPartitionsToTxn > v2 response', () => {
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
