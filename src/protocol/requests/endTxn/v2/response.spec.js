const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/endTxnResponse-v2-${caseId}.json`)

/**
 * Writes a v2 response body from a vector's values: the other direction of the byte check.
 * @param {{ throttleTime: number, errorCode: number }} fields
 * @returns {Buffer}
 */
const encodeResponse = ({ throttleTime, errorCode }) =>
  new Encoder().writeInt32(throttleTime).writeInt16(errorCode).buffer

describe('Protocol > Requests > EndTxn > v2 response', () => {
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

  test('parse resolves without an error code and throws PRODUCER_FENCED from the error vector', async () => {
    const ok = await decode(Buffer.from(vector('ok').hex, 'hex'))
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(ok)).resolves.toEqual(ok)
    await expect(parse(failed)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'PRODUCER_FENCED',
      code: 90,
      retriable: false,
    })
  })
})
