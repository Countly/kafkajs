const Decoder = require('../../../decoder')
const RequestV2Protocol = require('./request')
const { protocol } = require('../index')

const vector = (caseId) => require(`../fixtures/java/endTxnRequest-v2-${caseId}.json`)

/**
 * Reads a v2 request body back into the builder's arguments: the other direction of the byte check.
 * @param {Buffer} buffer
 * @returns {{ transactionalId: string, producerId: string, producerEpoch: number, transactionResult: boolean }}
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const fields = {
    transactionalId: decoder.readString(),
    producerId: decoder.readInt64().toString(),
    producerEpoch: decoder.readInt16(),
    transactionResult: decoder.readBoolean(),
  }
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

describe('Protocol > Requests > EndTxn > v2', () => {
  test.each(['commit', 'abort'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV2Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test.each(['commit', 'abort'])(
    'the %s vector decodes back to the values it was built from',
    (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
    }
  )

  test('is the v1 body: v2 only lets the broker answer PRODUCER_FENCED', async () => {
    const { hex, fields } = vector('commit')
    const v1 = await require('../v1/request')(fields).encode()

    expect(v1.buffer.toString('hex')).toEqual(hex)
  })

  test.each(['commit', 'abort'])(
    'announces version 2 and is reachable through the version map as %s',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { request } = protocol({ version: 2 })(fields)

      expect(request).toMatchObject({ apiName: 'EndTxn', apiVersion: 2 })
      expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
    }
  )
})
