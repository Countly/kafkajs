const { decode, parse } = require('./response')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > SaslAuthenticate > v2', () => {
  test('authBytes come back INT32-prefixed, exactly as v0 hands them to a provider', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v2_response.json')))

    expect(data.errorCode).toEqual(0)
    expect(data.errorMessage).toBeNull()
    expect(data.sessionLifetimeMs).toEqual('3600000')

    // A custom provider's response.decode(rawData) reads that prefix; this is the frozen half of
    // the contract, and it is why the codec re-wraps instead of handing over raw bytes.
    const decoder = new Decoder(data.authBytes)
    expect(decoder.readBytes()).toEqual(Buffer.from('server-final-message'))

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('an authentication failure carries its code and fixed text, not the broker message', async () => {
    const data = {
      errorCode: 58,
      errorMessage: 'Invalid username or password',
      authBytes: Buffer.alloc(0),
    }

    await expect(parse(data)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'SASL_AUTHENTICATION_FAILED',
      code: 58,
      message: 'SASL Authentication failed',
    })
  })

  test('decodes the vector Apache own codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v2_response_java.json')))

    expect(data.errorCode).toEqual(0)
    expect(data.errorMessage).toBeNull()
    expect(data.sessionLifetimeMs).toEqual('3600000')
    expect(new Decoder(data.authBytes).readBytes()).toEqual(Buffer.from('server-final-message'))
  })
})
