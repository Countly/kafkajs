const { decode, parse } = require('./response')

describe('Protocol > Requests > SaslAuthenticate > v1', () => {
  describe('PLAIN', () => {
    test('response', async () => {
      const data = await decode(Buffer.from(require('../fixtures/v1_response_plain.json')))
      expect(data).toEqual({
        authBytes: Buffer.from({ data: [0, 0, 0, 0], type: 'Buffer' }),
        errorCode: 0,
        errorMessage: null,
        sessionLifetimeMs: '360000',
      })

      await expect(parse(data)).resolves.toBeTruthy()
    })
  })

  describe('SCRAM', () => {
    test('response', async () => {
      const data = await decode(Buffer.from(require('../fixtures/v1_response_scram256.json')))
      expect(data).toEqual({
        authBytes: Buffer.from(require('../fixtures/scram256_firstRequest_response_v1.json')),
        errorCode: 0,
        errorMessage: null,
        sessionLifetimeMs: '360000',
      })

      await expect(parse(data)).resolves.toBeTruthy()
    })
  })

  describe('parse', () => {
    const SASL_AUTHENTICATION_FAILED = 58
    it('throws the fixed text of SASL_AUTHENTICATION_FAILED, not the broker message', async () => {
      const data = { errorCode: SASL_AUTHENTICATION_FAILED, errorMessage: 'Auth failed' }
      await expect(parse(data)).rejects.toMatchObject({
        name: 'KafkaJSProtocolError',
        type: 'SASL_AUTHENTICATION_FAILED',
        code: SASL_AUTHENTICATION_FAILED,
        message: 'SASL Authentication failed',
      })
    })
  })
})
