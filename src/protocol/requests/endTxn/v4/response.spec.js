const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/endTxnResponse-v4-${caseId}.json`)

/**
 * Writes a v4 response body from a vector's values: the other direction of the byte check.
 * @param {{ throttleTime: number, errorCode: number }} fields
 * @returns {Buffer}
 */
const encodeResponse = ({ throttleTime, errorCode }) =>
  new Encoder().writeInt32(throttleTime).writeInt16(errorCode).writeTaggedFields().buffer

describe('Protocol > Requests > EndTxn > v4 response', () => {
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

  test('parse throws TRANSACTION_ABORTABLE, the code v4 lets the broker send', async () => {
    await expect(
      parse({ throttleTime: 0, clientSideThrottleTime: 0, errorCode: 120 })
    ).rejects.toMatchObject({ type: 'TRANSACTION_ABORTABLE', code: 120, retriable: false })
  })
})
