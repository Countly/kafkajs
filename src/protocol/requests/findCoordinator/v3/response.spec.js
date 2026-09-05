const { decode, parse } = require('./response')

describe('Protocol > Requests > FindCoordinator > v3', () => {
  test('decodes the same shape as v2, from compact encoding', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response.json')))
    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      errorCode: 0,
      errorMessage: null,
      coordinator: { nodeId: 1, host: '127.0.0.1', port: 9092 },
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('throws KafkaJSProtocolError if the api is not supported', async () => {
    await expect(
      decode(Buffer.from(require('../fixtures/v1_response_version_error.json')))
    ).rejects.toThrow(/The version of API is not supported/)
  })
})
