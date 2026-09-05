const RequestV3Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > FindCoordinator > v3', () => {
  test('encodes Key as a compact string, KeyType as INT8, then an empty tag buffer', async () => {
    const { buffer } = await RequestV3Protocol({
      coordinatorKey: 'group-id',
      coordinatorType: 0,
    }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readUVarIntString()).toEqual('group-id')
    expect(decoder.readInt8()).toEqual(0)
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV3Protocol({
      coordinatorKey: 'test-group',
      coordinatorType: 0,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v3_request_java.json')))
  })
})
