const Decoder = require('../../../decoder')
const RequestV3Protocol = require('./request')
const { protocol } = require('../index')

const CASES = ['firstInit', 'reinit', 'nullTransactionalId']
const vector = (caseId) => require(`../fixtures/java/initProducerIdRequest-v3-${caseId}.json`)

/**
 * Reads a v3 request body back into the builder's arguments: the other direction of the byte check.
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

describe('Protocol > Requests > InitProducerId > v3', () => {
  test('sends producerId and producerEpoch as -1', async () => {
    const { buffer } = await RequestV3Protocol({
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
    const { buffer } = await RequestV3Protocol({ transactionTimeout: 5000 }).encode()
    expect(buffer[0]).toEqual(0) // compact null string
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV3Protocol({
      transactionalId: 'test-transactional-id',
      transactionTimeout: 30000,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v3_request_java.json')))
  })

  test.each(CASES)('encodes the Apache 4.3.1 %s vector byte for byte', async (caseId) => {
    const { hex, fields } = vector(caseId)
    const { buffer } = await RequestV3Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test.each(CASES)('the %s vector decodes back to the values it was built from', (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
  })

  test('encodes zero producerId and producerEpoch as given, not -1 defaults', async () => {
    const values = {
      transactionalId: 'test-transactional-id',
      transactionTimeout: 30000,
      producerId: 0,
      producerEpoch: 0,
    }
    const { buffer } = await RequestV3Protocol(values).encode()

    expect(decodeRequest(buffer)).toEqual({
      transactionalId: 'test-transactional-id',
      transactionTimeout: 30000,
      producerId: '0',
      producerEpoch: 0,
    })
  })

  test('takes the producer id as a number as well as the string a response decodes to', async () => {
    const { hex, fields } = vector('reinit')
    const { buffer } = await RequestV3Protocol({ ...fields, producerId: 1000 }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('keeps every bit of a producer id beyond Number.MAX_SAFE_INTEGER', async () => {
    const values = {
      transactionalId: null,
      transactionTimeout: 30000,
      producerId: '9223372036854775807',
      producerEpoch: 32767,
    }
    const { buffer } = await RequestV3Protocol(values).encode()

    expect(decodeRequest(buffer)).toEqual(values)
  })

  test('the version map passes the producer id and epoch through', async () => {
    const { hex, fields } = vector('reinit')
    const { request } = protocol({ version: 3 })(fields)

    expect(request).toMatchObject({ apiName: 'InitProducerId', apiVersion: 3 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('through the version map, a caller that names neither sends a first initialization', async () => {
    const { hex, fields } = vector('firstInit')
    const { request } = protocol({ version: 3 })({
      transactionalId: fields.transactionalId,
      transactionTimeout: fields.transactionTimeout,
    })

    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('below v3 the version map drops the pair: that body has no field for it', async () => {
    const { fields } = vector('reinit')
    const withPair = protocol({ version: 2 })(fields).request
    const withoutPair = protocol({ version: 2 })({
      transactionalId: fields.transactionalId,
      transactionTimeout: fields.transactionTimeout,
    }).request

    expect((await withPair.encode()).buffer).toEqual((await withoutPair.encode()).buffer)
  })
})
