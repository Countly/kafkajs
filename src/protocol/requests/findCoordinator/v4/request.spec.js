const RequestV4Protocol = require('./request')
const Decoder = require('../../../decoder')
const COORDINATOR_TYPES = require('../../../coordinatorTypes')

describe('Protocol > Requests > FindCoordinator > v4', () => {
  test('sends the single coordinator key in the CoordinatorKeys array', async () => {
    const { buffer } = await RequestV4Protocol({
      coordinatorKey: 'test-group',
      coordinatorType: COORDINATOR_TYPES.GROUP,
    }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readInt8()).toEqual(COORDINATOR_TYPES.GROUP)
    expect(decoder.readUVarIntArray((d) => d.readUVarIntString())).toEqual(['test-group'])
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('does not send the pre-v4 Key field', async () => {
    const { buffer } = await RequestV4Protocol({ coordinatorKey: 'g', coordinatorType: 1 }).encode()
    // KeyType is the first byte at v4; at v3 the first byte is the compact string length of Key.
    expect(buffer[0]).toEqual(1)
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV4Protocol({
      coordinatorKey: 'test-group',
      coordinatorType: 0,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v4_request_java.json')))
  })
})
