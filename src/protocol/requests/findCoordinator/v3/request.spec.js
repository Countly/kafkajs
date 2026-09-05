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
})
