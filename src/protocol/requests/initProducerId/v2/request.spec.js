const Decoder = require('../../../decoder')
const RequestV2Protocol = require('./request')

describe('Protocol > Requests > InitProducerId > v2', () => {
  test('request', async () => {
    const { buffer } = await RequestV2Protocol({
      transactionalId: 'test-transactional-id',
      transactionTimeout: 30000,
    }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readUVarIntString()).toEqual('test-transactional-id')
    expect(decoder.readInt32()).toEqual(30000)
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('a non-transactional producer sends a null transactional id', async () => {
    const { buffer } = await RequestV2Protocol({ transactionTimeout: 5000 }).encode()
    expect(buffer[0]).toEqual(0) // compact null string
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV2Protocol({
      transactionalId: 'test-transactional-id',
      transactionTimeout: 30000,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v2_request_java.json')))
  })
})
