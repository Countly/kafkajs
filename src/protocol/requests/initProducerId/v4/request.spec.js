const Decoder = require('../../../decoder')
const RequestV4Protocol = require('./request')
const requestV3 = require('../v3/request')
const { protocol } = require('../index')

const CASES = ['firstInit', 'reinit', 'nullTransactionalId']
const vector = (caseId) => require(`../fixtures/java/initProducerIdRequest-v4-${caseId}.json`)

/**
 * Reads a v4 request body back into the builder's arguments: the other direction of the byte check.
 * @param {Buffer} buffer
 * @returns {{ transactionalId: string|null, transactionTimeout: number, producerId: string, producerEpoch: number }}
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const fields = {
    transactionalId: decoder.readUVarIntString(),
    transactionTimeout: decoder.readInt32(),
    producerId: decoder.readInt64().toString(),
    producerEpoch: decoder.readInt16(),
  }
  expect(decoder.readTaggedFields().size).toEqual(0)
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

describe('Protocol > Requests > InitProducerId > v4', () => {
  test('sends producerId and producerEpoch as -1', async () => {
    const { buffer } = await RequestV4Protocol({
      transactionalId: 'test-transactional-id',
      transactionTimeout: 30000,
    }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readUVarIntString()).toEqual('test-transactional-id')
    expect(decoder.readInt32()).toEqual(30000)
    expect(decoder.readInt64().toString()).toEqual('-1')
    expect(decoder.readInt16()).toEqual(-1)
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('a non-transactional producer sends a null transactional id', async () => {
    const { buffer } = await RequestV4Protocol({ transactionTimeout: 5000 }).encode()
    expect(buffer[0]).toEqual(0) // compact null string
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV4Protocol({
      transactionalId: 'test-transactional-id',
      transactionTimeout: 30000,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v4_request_java.json')))
  })

  test.each(CASES)('encodes the Apache 4.3.1 %s vector byte for byte', async (caseId) => {
    const { hex, fields } = vector(caseId)
    const { buffer } = await RequestV4Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test.each(CASES)('the %s vector decodes back to the values it was built from', (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
  })

  test('is the v3 body under its own version number', async () => {
    for (const caseId of CASES) {
      const { fields } = vector(caseId)
      const request = RequestV4Protocol(fields)

      expect(request.apiVersion).toEqual(4)
      expect((await request.encode()).buffer).toEqual((await requestV3(fields).encode()).buffer)
    }
  })

  test('the version map passes the producer id and epoch through', async () => {
    const { hex, fields } = vector('reinit')
    const { request } = protocol({ version: 4 })(fields)

    expect(request).toMatchObject({ apiName: 'InitProducerId', apiVersion: 4 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
