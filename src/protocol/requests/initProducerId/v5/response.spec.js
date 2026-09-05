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
})
