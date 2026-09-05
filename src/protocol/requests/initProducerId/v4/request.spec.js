const Decoder = require('../../../decoder')
const RequestV4Protocol = require('./request')

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
})
