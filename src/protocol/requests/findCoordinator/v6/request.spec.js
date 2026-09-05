const Decoder = require('../../../decoder')
const COORDINATOR_TYPES = require('../../../coordinatorTypes')
const RequestV4Protocol = require('../v4/request')
const RequestV6Protocol = require('./request')

describe('Protocol > Requests > FindCoordinator > v6', () => {
  test('apiVersion is 6', () => {
    expect(RequestV6Protocol({ coordinatorKey: 'g', coordinatorType: 0 }).apiVersion).toEqual(6)
  })

  test('reuses v4 encoding byte-for-byte', async () => {
    const options = { coordinatorKey: 'test-group', coordinatorType: 1 }
    const v4 = await RequestV4Protocol(options).encode()
    const v6 = await RequestV6Protocol(options).encode()
    expect(v6.buffer).toEqual(v4.buffer)
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV6Protocol({
      coordinatorKey: 'test-group',
      coordinatorType: 0,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v6_request_java.json')))
  })

  test.each([
    ['4.3.1', '../fixtures/v6_request_kafka431.json'],
    ['4.1.2', '../fixtures/v6_request_kafka412.json'],
  ])('is the request a real Kafka %s cluster answered', (_version, fixture) => {
    const decoder = new Decoder(Buffer.from(require(fixture)))

    expect(decoder.readInt8()).toEqual(COORDINATOR_TYPES.GROUP)
    expect(decoder.readUVarIntArray((d) => d.readUVarIntString())).toEqual([
      'kafkajs-capture-group',
    ])
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})
