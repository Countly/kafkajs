const { decode, parse } = require('./response')
const { unsupportedVersionResponseWithTimeout } = require('testHelpers')

describe('Protocol > Requests > InitProducerId > v2', () => {
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
})
