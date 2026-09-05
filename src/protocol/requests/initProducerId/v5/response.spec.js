const { decode, parse } = require('./response')
const { unsupportedVersionResponseWithTimeout } = require('testHelpers')

describe('Protocol > Requests > InitProducerId > v5', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v2_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      producerId: '1000',
      producerEpoch: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('throws KafkaJSProtocolError if the api is not supported', async () => {
    await expect(decode(unsupportedVersionResponseWithTimeout())).rejects.toThrow(
      /The version of API is not supported/
    )
  })

  test.each([
    ['PRODUCER_FENCED', 90],
    ['TRANSACTION_ABORTABLE', 120],
  ])('%s surfaces as the non-retriable protocol error it already is', async (type, code) => {
    const data = await decode(Buffer.from(require('../fixtures/v2_response.json')))
    data.errorCode = code
    await expect(parse(data)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type,
      code,
      retriable: false,
    })
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v5_response_java.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      producerId: '1000',
      producerEpoch: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test.each([
    ['4.3.1', '../fixtures/v5_response_kafka431.json'],
    ['4.1.2', '../fixtures/v5_response_kafka412.json'],
  ])('decodes a real broker response from Kafka %s', async (_version, fixture) => {
    const data = await decode(Buffer.from(require(fixture)))

    expect(data.errorCode).toEqual(0)
    expect(Number(data.producerId)).toBeGreaterThan(0)
    expect(data.producerEpoch).toBeGreaterThanOrEqual(0)

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
