const Decoder = require('../../../decoder')
const RequestV3Protocol = require('./request')
const { protocol } = require('../index')

const vector = (caseId) => require(`../fixtures/java/endTxnRequest-v3-${caseId}.json`)

/**
 * Reads a v3 request body back into the builder's arguments: a compact string and a closing tag
 * buffer.
 * @param {Buffer} buffer
 * @returns {{ transactionalId: string, producerId: string, producerEpoch: number, transactionResult: boolean }}
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const fields = {
    transactionalId: decoder.readUVarIntString(),
    producerId: decoder.readInt64().toString(),
    producerEpoch: decoder.readInt16(),
    transactionResult: decoder.readBoolean(),
  }
  expect(decoder.readTaggedFields().size).toEqual(0)
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

describe('Protocol > Requests > EndTxn > v3', () => {
  test.each(['commit', 'abort'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV3Protocol(fields).encode()

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

  test('announces version 3 and is reachable through the version map', async () => {
    const { hex, fields } = vector('commit')
    const { request } = protocol({ version: 3 })(fields)

    expect(request).toMatchObject({ apiName: 'EndTxn', apiVersion: 3 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
