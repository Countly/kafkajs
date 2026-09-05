const Decoder = require('../../../decoder')
const RequestV5Protocol = require('./request')

describe('Protocol > Requests > InitProducerId > v5', () => {
  test('sends producerId and producerEpoch as -1', async () => {
    const { buffer } = await RequestV5Protocol({
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
    const { buffer } = await RequestV5Protocol({ transactionTimeout: 5000 }).encode()
    expect(buffer[0]).toEqual(0) // compact null string
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV5Protocol({
      transactionalId: 'test-transactional-id',
      transactionTimeout: 30000,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v5_request_java.json')))
  })

  test.each([
    ['4.3.1', '../fixtures/v5_request_kafka431.json'],
    ['4.1.2', '../fixtures/v5_request_kafka412.json'],
  ])('is the request a real Kafka %s cluster answered', (_version, fixture) => {
    const decoder = new Decoder(Buffer.from(require(fixture)))

    expect(decoder.readUVarIntString()).toEqual('kafkajs-capture-txn')
    expect(decoder.readInt32()).toBeGreaterThan(0) // transactionTimeoutMs
    expect(decoder.readInt64().toString()).toEqual('-1') // producerId
    expect(decoder.readInt16()).toEqual(-1) // producerEpoch
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})
