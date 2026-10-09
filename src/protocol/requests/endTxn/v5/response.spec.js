const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/endTxnResponse-v5-${caseId}.json`)

/**
 * Writes a v5 response body from a vector's values: the other direction of the byte check.
 * @param {{ throttleTime: number, errorCode: number, producerId: string, producerEpoch: number }} fields
 * @returns {Buffer}
 */
const encodeResponse = ({ throttleTime, errorCode, producerId, producerEpoch }) =>
  new Encoder()
    .writeInt32(throttleTime)
    .writeInt16(errorCode)
    .writeInt64(producerId)
    .writeInt16(producerEpoch)
    .writeTaggedFields().buffer

describe('Protocol > Requests > EndTxn > v5 response', () => {
  test.each(['producerIdAndEpoch', 'defaults', 'error'])(
    'decodes the Apache 4.3.1 %s vector, producer id and epoch included',
    async (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        errorCode: fields.errorCode,
        producerId: fields.producerId,
        producerEpoch: fields.producerEpoch,
      })
    }
  )

  test.each(['producerIdAndEpoch', 'defaults', 'error'])(
    'the %s values encode to the vector bytes',
    (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(encodeResponse(fields).toString('hex')).toEqual(hex)
    }
  )

  test('a coordinator with no pair to report sends -1 and -1, the fields defaults', async () => {
    const data = await decode(Buffer.from(vector('defaults').hex, 'hex'))

    expect(data).toMatchObject({ errorCode: 0, producerId: '-1', producerEpoch: -1 })
    await expect(parse(data)).resolves.toEqual(data)
  })

  test('parse resolves the bumped pair and throws PRODUCER_FENCED from the error vector', async () => {
    const bumped = await decode(Buffer.from(vector('producerIdAndEpoch').hex, 'hex'))
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(bumped)).resolves.toMatchObject({ producerId: '1000', producerEpoch: 4 })
    await expect(parse(failed)).rejects.toMatchObject({ type: 'PRODUCER_FENCED', code: 90 })
  })

  test('rejects UNSUPPORTED_VERSION while decoding', async () => {
    const buffer = Buffer.from(vector('defaults').hex, 'hex')
    buffer.writeInt16BE(35, 4)

    await expect(decode(buffer)).rejects.toMatchObject({ type: 'UNSUPPORTED_VERSION', code: 35 })
  })
})
