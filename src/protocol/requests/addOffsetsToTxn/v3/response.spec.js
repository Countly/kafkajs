const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/addOffsetsToTxnResponse-v3-${caseId}.json`)

/**
 * Writes a v3 response body from a vector's values: the other direction of the byte check.
 * @param {{ throttleTime: number, errorCode: number }} fields
 * @returns {Buffer}
 */
const encodeResponse = ({ throttleTime, errorCode }) =>
  new Encoder().writeInt32(throttleTime).writeInt16(errorCode).writeTaggedFields().buffer

describe('Protocol > Requests > AddOffsetsToTxn > v3 response', () => {
  test.each(['ok', 'error'])(
    'decodes the Apache 4.3.1 %s vector into the v1 shape',
    async (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        errorCode: fields.errorCode,
      })
    }
  )

  test.each(['ok', 'error'])('the %s values encode to the vector bytes', (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(encodeResponse(fields).toString('hex')).toEqual(hex)
  })

  test('rejects a body that ends before its tag buffer', async () => {
    const { fields } = vector('ok')

    await expect(decode(encodeResponse(fields).subarray(0, -1))).rejects.toMatchObject({
      name: 'KafkaJSPartialMessageError',
    })
  })

  test('rejects UNSUPPORTED_VERSION while decoding, as every earlier version does', async () => {
    const buffer = Buffer.from(vector('ok').hex, 'hex')
    buffer.writeInt16BE(35, 4)

    await expect(decode(buffer)).rejects.toMatchObject({ type: 'UNSUPPORTED_VERSION', code: 35 })
  })

  test('parse resolves without an error code and throws with one', async () => {
    const ok = await decode(Buffer.from(vector('ok').hex, 'hex'))
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(ok)).resolves.toEqual(ok)
    await expect(parse(failed)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'CONCURRENT_TRANSACTIONS',
      code: 51,
    })
  })
})
